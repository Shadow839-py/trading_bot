"""Regression tests for complete Deribit option snapshot indexing."""
import unittest
from datetime import datetime, timedelta, timezone
from analytics import OptionObservation, index_option_snapshot, calculate_gamma_exposure

class OptionSnapshotTests(unittest.TestCase):
    def setUp(self):
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(days=14)
        self.call = OptionObservation(84000, "call", 0.55, 100, expiry, "Deribit", now)
        self.put = OptionObservation(84000, "put", 0.60, 120, expiry, "Deribit", now)

    def test_duplicate_instrument_is_counted_once(self):
        rows = index_option_snapshot([("BTC-14NOV26-84000-C", self.call),
                                      ("BTC-14NOV26-84000-C", self.call),
                                      ("BTC-14NOV26-84000-P", self.put)])
        self.assertEqual(len(rows), 2)
        result = calculate_gamma_exposure(rows.values(), 83500, datetime.now(timezone.utc))
        self.assertEqual(len(result.levels), 1)

    def test_latest_duplicate_replaces_previous(self):
        newer = OptionObservation(84000, "call", 0.50, 150, self.call.expiry_utc, "Deribit", datetime.now(timezone.utc))
        rows = index_option_snapshot([("same", self.call), ("same", newer)])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows["same"].open_interest, 150)

    def test_blank_instrument_is_ignored(self):
        rows = index_option_snapshot([("", self.call), ("  ", self.put)])
        self.assertEqual(rows, {})

if __name__ == "__main__":
    unittest.main(verbosity=2)
