import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from extractor.app import main


class ExtractionMetadataTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_landing_dir = main.LANDING_DIR
        main.LANDING_DIR = Path(self.temp_dir.name)
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        self.addCleanup(self.temp_dir.cleanup)
        self.addCleanup(setattr, main, "LANDING_DIR", self.original_landing_dir)

    def test_incremental_request_uses_checkpoint_and_records_new_watermark(self):
        requested_urls = []
        payload = [
            {"id": 1, "updated_at": "2026-10-01T15:00:00+03:00"},
            {"id": 2, "updated_at": "2026-10-01T11:30:00+00:00"},
        ]

        def fake_get(url, **kwargs):
            requested_urls.append(url)
            return httpx.Response(
                200, json=payload, request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={
                    "source": "items",
                    "url": "https://example.test/items",
                    "load_mode": "incremental_upsert",
                    "checkpoint_before": "2026-10-01T10:00:00+00:00",
                    "incremental_param": "changed_after",
                    "watermark_field": "updated_at",
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            httpx.URL(requested_urls[0]).params["changed_after"],
            "2026-10-01T10:00:00+00:00",
        )
        result = response.json()
        manifest = json.loads((Path(self.temp_dir.name) / result["manifest_filename"]).read_text())
        self.assertEqual(result["checkpoint_after"], "2026-10-01T12:00:00+00:00")
        self.assertTrue(result["pagination_complete"])
        self.assertEqual(manifest["load_mode"], "incremental_upsert")
        self.assertEqual(manifest["checkpoint_before"], "2026-10-01T10:00:00+00:00")

    def test_incremental_without_watermark_fetches_endpoint_without_checkpoint(self):
        requested_urls = []

        def fake_get(url, **kwargs):
            requested_urls.append(str(url))
            return httpx.Response(
                200, json=[{"id": 1, "name": "current"}],
                request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        url = "https://example.test/items?view=latest"
        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={
                    "source": "items",
                    "url": url,
                    "load_mode": "incremental_upsert",
                    "incremental_param": "",
                    "watermark_field": "",
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(requested_urls, [url])
        self.assertIsNone(response.json()["checkpoint_after"])
        manifest = json.loads(
            (Path(self.temp_dir.name) / response.json()["manifest_filename"]).read_text()
        )
        self.assertIsNone(manifest["checkpoint_after"])

    def test_dummyjson_offset_pagination_fetches_and_combines_every_page(self):
        payload_by_skip = {
            0: {"products": [{"id": 1}], "total": 3, "skip": 0, "limit": 1},
            1: {"products": [{"id": 2}], "total": 3, "skip": 1, "limit": 1},
            2: {"products": [{"id": 3}], "total": 3, "skip": 2, "limit": 1},
        }
        requested_urls = []

        def fake_get(url, **kwargs):
            requested_urls.append(str(url))
            skip = int(httpx.URL(url).params.get("skip", "0"))
            return httpx.Response(
                200, json=payload_by_skip[skip], request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={"source": "items", "url": "https://example.test/items?limit=1"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        payload_file = Path(self.temp_dir.name) / result["filename"]
        combined = json.loads(payload_file.read_text())
        self.assertEqual([product["id"] for product in combined["products"]], [1, 2, 3])
        self.assertEqual(
            [httpx.URL(url).params.get("skip", "0") for url in requested_urls],
            ["0", "1", "2"],
        )
        self.assertTrue(result["pagination_complete"])

    def test_link_header_pagination_follows_relative_next_link(self):
        requested_urls = []

        def fake_get(url, **kwargs):
            requested_urls.append(str(url))
            page = httpx.URL(url).params.get("page", "1")
            headers = {"content-type": "application/json"}
            if page == "1":
                headers["link"] = '</items?page=2>; rel="next"'
            return httpx.Response(
                200, json={"items": [{"id": int(page)}]},
                request=httpx.Request("GET", url), headers=headers,
            )

        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={"source": "items", "url": "https://example.test/items?page=1"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        combined = json.loads((Path(self.temp_dir.name) / response.json()["filename"]).read_text())
        self.assertEqual([item["id"] for item in combined["items"]], [1, 2])
        self.assertEqual(len(requested_urls), 2)
        self.assertTrue(response.json()["pagination_complete"])

    def test_json_next_url_pagination_is_followed(self):
        def fake_get(url, **kwargs):
            page = httpx.URL(url).params.get("page", "1")
            payload = {"results": [{"id": int(page)}], "next": "/items?page=2" if page == "1" else None}
            return httpx.Response(
                200, json=payload, request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={"source": "items", "url": "https://example.test/items?page=1"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        combined = json.loads((Path(self.temp_dir.name) / response.json()["filename"]).read_text())
        self.assertEqual([item["id"] for item in combined["results"]], [1, 2])
        self.assertTrue(response.json()["pagination_complete"])

    def test_has_more_pagination_advances_page_number(self):
        requested_pages = []

        def fake_get(url, **kwargs):
            page = int(httpx.URL(url).params.get("page", "1"))
            requested_pages.append(page)
            return httpx.Response(
                200,
                json={"items": [{"id": page}], "has_more": page == 1},
                request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={"source": "items", "url": "https://example.test/items"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        combined = json.loads((Path(self.temp_dir.name) / response.json()["filename"]).read_text())
        self.assertEqual([item["id"] for item in combined["items"]], [1, 2])
        self.assertEqual(requested_pages, [1, 2])
        self.assertTrue(response.json()["pagination_complete"])

    def test_unrecognized_cursor_pagination_is_marked_incomplete(self):
        def fake_get(url, **kwargs):
            return httpx.Response(
                200,
                json={"items": [{"id": 1}], "next_cursor": "cursor-2"},
                request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={"source": "items", "url": "https://example.test/items"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["pagination_complete"])

    def test_pagination_next_host_must_pass_allowlist(self):
        requested_urls = []

        def fake_get(url, **kwargs):
            requested_urls.append(str(url))
            return httpx.Response(
                200, json={"items": [{"id": 1}], "next": "https://untrusted.example/items"},
                request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        with patch.object(
            main, "is_allowed_url",
            side_effect=lambda url: "example.test" in url,
        ), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={"source": "items", "url": "https://example.test/items"},
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(requested_urls, ["https://example.test/items"])

    def test_redirect_target_must_pass_allowlist(self):
        requested_urls = []

        def fake_get(url, **kwargs):
            requested_urls.append(str(url))
            return httpx.Response(
                302,
                headers={"location": "https://untrusted.example/items"},
                request=httpx.Request("GET", url),
            )

        with patch.object(
            main, "is_allowed_url",
            side_effect=lambda url: "example.test" in url,
        ), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={"source": "items", "url": "https://example.test/items"},
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(requested_urls, ["https://example.test/items"])

    def test_repeated_pagination_url_fails_instead_of_looping(self):
        requested_urls = []

        def fake_get(url, **kwargs):
            requested_urls.append(str(url))
            return httpx.Response(
                200,
                json={"items": [{"id": 1}], "next": "/items?page=1"},
                request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={"source": "items", "url": "https://example.test/items?page=1"},
            )

        self.assertEqual(response.status_code, 502)
        self.assertIn("repeated URL", response.json()["detail"])
        self.assertEqual(len(requested_urls), 1)

    def test_pagination_page_limit_stops_fetching(self):
        requested_urls = []

        def fake_get(url, **kwargs):
            requested_urls.append(str(url))
            page = int(httpx.URL(url).params.get("page", "1"))
            return httpx.Response(
                200,
                json={"items": [{"id": page}], "next": f"/items?page={page + 1}"},
                request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        with (
            patch.object(main, "MAX_PAGINATION_PAGES", 2),
            patch.object(main, "is_allowed_url", return_value=True),
            patch.object(httpx, "get", fake_get),
        ):
            response = self.client.post(
                "/extract",
                json={"source": "items", "url": "https://example.test/items?page=1"},
            )

        self.assertEqual(response.status_code, 502)
        self.assertIn("safety limit", response.json()["detail"])
        self.assertEqual(len(requested_urls), 2)

    def test_empty_incremental_response_reuses_checkpoint(self):
        def fake_get(url, **kwargs):
            return httpx.Response(
                200, json=[], request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        checkpoint = "2026-10-01T10:00:00+00:00"
        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={
                    "source": "items",
                    "url": "https://example.test/items",
                    "load_mode": "incremental_upsert",
                    "checkpoint_before": checkpoint,
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["checkpoint_after"], checkpoint)

    def test_incremental_checkpoint_does_not_move_backwards(self):
        def fake_get(url, **kwargs):
            return httpx.Response(
                200, json=[{"id": 1, "updated_at": "2026-10-01T09:00:00+00:00"}],
                request=httpx.Request("GET", url),
                headers={"content-type": "application/json"},
            )

        checkpoint = "2026-10-01T10:00:00+00:00"
        with patch.object(main, "is_allowed_url", return_value=True), patch.object(httpx, "get", fake_get):
            response = self.client.post(
                "/extract",
                json={
                    "source": "items",
                    "url": "https://example.test/items",
                    "load_mode": "incremental_upsert",
                    "checkpoint_before": checkpoint,
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["checkpoint_after"], checkpoint)


if __name__ == "__main__":
    unittest.main()
