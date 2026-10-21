"""Offline smoke tests for the Phase 3–4 integration fixes.

Run with: python -m unittest -v test_integration_fixes.py
These tests do not require exchange connectivity or a database driver.
"""
import asyncio
import unittest

from broker import BrokerConfig, BrokerError, Order, OrderSide, OrderType, PaperBroker
from strategy import ConfluenceEngine, StrategyConfig, SignalSide


class PaperProtectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.broker = PaperBroker(BrokerConfig(starting_cash=100_000.0))
        await self.broker.update_market("BTCUSDT", 60_000.0, 59_999.0, 60_001.0, timestamp=1_000.0)

    async def test_public_protection_api_for_long(self):
        order = await self.broker.place_order(Order(
            symbol="BTCUSDT", side=OrderSide.BUY, quantity_btc=0.001,
            order_type=OrderType.MARKET, client_order_id="test-long"))
        self.assertEqual(order.status.value, "filled")
        pos = await self.broker.get_position("BTCUSDT")
        updated = await self.broker.set_position_protection(
            "BTCUSDT", stop_price=pos.average_entry_price - 400,
            target_price=pos.average_entry_price + 800)
        self.assertLess(updated.stop_price, updated.average_entry_price)
        self.assertGreater(updated.target_price, updated.average_entry_price)

    async def test_protection_rejects_invalid_long_geometry(self):
        await self.broker.place_order(Order(
            symbol="BTCUSDT", side=OrderSide.BUY, quantity_btc=0.001,
            order_type=OrderType.MARKET, client_order_id="test-invalid"))
        pos = await self.broker.get_position("BTCUSDT")
        with self.assertRaises(BrokerError):
            await self.broker.set_position_protection(
                "BTCUSDT", stop_price=pos.average_entry_price + 100,
                target_price=pos.average_entry_price + 800)

    async def test_protection_requires_open_position(self):
        with self.assertRaises(BrokerError):
            await self.broker.set_position_protection("BTCUSDT", stop_price=59_000, target_price=61_000)


class StrategySmokeTests(unittest.TestCase):
    def test_missing_inputs_fail_closed_to_wait(self):
        engine = ConfluenceEngine(StrategyConfig(symbol="BTCUSDT"))
        signal = engine.evaluate(price=60_000, gamma=None, walls=[], delta_bar=None,
                                 candles=[], now=10_000, has_open_position=False)
        self.assertEqual(signal.side, SignalSide.WAIT)
        self.assertIn("Gamma", signal.reason)


if __name__ == "__main__":
    unittest.main(verbosity=2)
