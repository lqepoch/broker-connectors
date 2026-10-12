"""Unit tests for reproducible SPDX SBOM metadata."""

from __future__ import annotations

import importlib.util
import os
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "generate_spdx_sbom", ROOT / "scripts/generate_spdx_sbom.py"
)
assert SPEC is not None and SPEC.loader is not None
GENERATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GENERATOR)


class CreationTimestampTests(unittest.TestCase):
    def test_fixed_epoch_has_stable_utc_timestamp(self) -> None:
        with patch.dict(os.environ, {"SOURCE_DATE_EPOCH": "0"}):
            self.assertEqual(GENERATOR.creation_timestamp(), "1970-01-01T00:00:00Z")

    def test_invalid_epoch_values_are_rejected(self) -> None:
        for value in ("", "-1", "1.5", "１２３"):
            with self.subTest(value=value), patch.dict(
                os.environ, {"SOURCE_DATE_EPOCH": value}
            ):
                with self.assertRaises(ValueError):
                    GENERATOR.creation_timestamp()

    def test_epoch_outside_supported_range_is_rejected(self) -> None:
        with patch.dict(
            os.environ, {"SOURCE_DATE_EPOCH": "9" * 100}
        ), self.assertRaises(ValueError):
            GENERATOR.creation_timestamp()


if __name__ == "__main__":
    unittest.main()
