"""Database integration tests; requires aiosqlite from requirements.txt.

Run with: python -m unittest -v test_database_integration.py
"""
import tempfile
import unittest
from pathlib import Path

from broker import BrokerConfig, Order, OrderSide, OrderType, PaperBroker
from database import Database


class DatabaseIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "integration.db"
        self.db = Database(f"sqlite+aiosqlite:///{db_path.as_posix()}")
        await self.db.init()

    async def asyncTearDown(self):
        await self.db.close()
        self.temp_dir.cleanup()

    async def test_trade_journal_idempotency_close_and_summary(self):
        trade = await self.db.create_trade(
            direction="long", symbol="BTCUSDT", quantity_btc=0.01,
            entry_price=60_000, entry_time=1_700_000_000,
            broker_order_id="order-entry-1", stop_price=59_600,
            target_price=60_800, fee_costs=0.30, setup_tag="test")
        duplicate = await self.db.create_trade(
            direction="long", symbol="BTCUSDT", quantity_btc=0.01,
            entry_price=60_000, entry_time=1_700_000_000,
            broker_order_id="order-entry-1", stop_price=59_600,
            target_price=60_800, fee_costs=0.30, setup_tag="test")
        self.assertEqual(trade["id"], duplicate["id"])
        closed = await self.db.close_trade(trade["id"], exit_price=60_400,
            realized_pnl_usd=3.70, exit_time=1_700_000_100, fee_costs=0.60)
        self.assertEqual(closed["status"], "closed")
        self.assertAlmostEqual(closed["r_multiple"], 0.925, places=3)
        summary = await self.db.summary()
        self.assertEqual(summary["closed_trades"], 1)
        self.assertAlmostEqual(summary["net_pnl_usd"], 3.70, places=2)

    async def test_paper_broker_snapshot_recovery(self):
        original = PaperBroker(BrokerConfig())
        await original.update_market("BTCUSDT", 60_000, 59_999, 60_001, timestamp=1_700_000_000)
        await original.place_order(Order(symbol="BTCUSDT", side=OrderSide.BUY,
            quantity_btc=0.001, order_type=OrderType.MARKET, client_order_id="recovery-entry"))
        await self.db.snapshot_broker(original)
        restored = PaperBroker(BrokerConfig())
        self.assertTrue(await self.db.recover_broker(restored))
        old_position = await original.get_position("BTCUSDT")
        new_position = await restored.get_position("BTCUSDT")
        self.assertAlmostEqual(old_position.quantity_btc, new_position.quantity_btc)
        self.assertAlmostEqual(old_position.average_entry_price, new_position.average_entry_price)
        self.assertEqual(len(await restored.get_fills()), len(await original.get_fills()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
