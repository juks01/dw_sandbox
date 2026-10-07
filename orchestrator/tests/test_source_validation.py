import unittest

from fastapi import HTTPException

from orchestrator.app.main import SourceCreate, _validate_source_settings


class SourceValidationTests(unittest.TestCase):
    def test_accepts_absolute_http_url_and_demo_fixture(self):
        for url in ("https://dummyjson.com/products", "local://demo"):
            with self.subTest(url=url):
                _validate_source_settings(SourceCreate(
                    name="products",
                    url=url,
                    cron="0 * * * *",
                ))

    def test_rejects_relative_source_name_as_url(self):
        with self.assertRaises(HTTPException) as context:
            _validate_source_settings(SourceCreate(
                name="products",
                url="dummy_products",
                cron="0 * * * *",
                load_mode="incremental_upsert",
            ))

        self.assertEqual(context.exception.status_code, 422)
        self.assertIn("absolute HTTP(S) URL", context.exception.detail)

    def test_accepts_incremental_upsert_without_watermark(self):
        _validate_source_settings(SourceCreate(
            name="items",
            url="https://example.test/items",
            cron="0 * * * *",
            load_mode="incremental_upsert",
            incremental_param="",
            watermark_field="",
        ))

    def test_watermark_requires_incremental_query_parameter(self):
        with self.assertRaises(HTTPException) as context:
            _validate_source_settings(SourceCreate(
                name="items",
                url="https://example.test/items",
                cron="0 * * * *",
                load_mode="incremental_upsert",
                incremental_param="",
                watermark_field="updated_at",
            ))

        self.assertEqual(context.exception.status_code, 422)
        self.assertIn("query parameter", context.exception.detail)


if __name__ == "__main__":
    unittest.main()
