import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from loader.app import main


class LoadManifestMetadataTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_landing_dir = main.LANDING_DIR
        main.LANDING_DIR = Path(self.temp_dir.name)
        self.addCleanup(self.temp_dir.cleanup)
        self.addCleanup(setattr, main, "LANDING_DIR", self.original_landing_dir)

    def test_batch_metadata_is_passed_from_manifest_to_staging(self):
        payload = [{"id": 1}]
        raw = json.dumps(payload).encode("utf-8")
        (Path(self.temp_dir.name) / "items.json").write_bytes(raw)
        manifest = {
            "payload_filename": "items.json",
            "source": "items",
            "run_id": "run-12",
            "requested_url": "https://example.test/items",
            "load_mode": "incremental_upsert",
            "delete_policy": "never_close",
            "checkpoint_before": "2026-10-01T10:00:00+00:00",
            "checkpoint_after": "2026-10-01T11:00:00+00:00",
            "pagination_complete": True,
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
        (Path(self.temp_dir.name) / "items.manifest.json").write_text(json.dumps(manifest))
        result = {"status": "loaded", "batch_id": 12, "tables": {"items": 1}}

        with patch.object(main.db, "load_payload", return_value=result) as load_payload:
            response = main.load(main.LoadRequest(
                filename="items.json",
                source="items",
                source_url=manifest["requested_url"],
                run_id=manifest["run_id"],
                load_mode=manifest["load_mode"],
                delete_policy=manifest["delete_policy"],
                checkpoint_before=manifest["checkpoint_before"],
                checkpoint_after=manifest["checkpoint_after"],
                pagination_complete=True,
            ))

        self.assertEqual(response["batch_id"], 12)
        args = load_payload.call_args.args
        self.assertEqual(args[1:4], ("items", manifest["requested_url"], payload))
        self.assertEqual(args[5:], (
            "incremental_upsert", "never_close", "run-12",
            manifest["checkpoint_before"], manifest["checkpoint_after"], True,
        ))


if __name__ == "__main__":
    unittest.main()
