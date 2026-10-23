import unittest
from generator.data_generator import FinTechDataGenerator


class TestFinTechGenerator(unittest.TestCase):
    def test_fintech_generator_dimensions(self):
        gen = FinTechDataGenerator(seed=123)
        gen.generate_dimensions(num_accounts=100, num_cards=200, num_merchants=50, num_devices=50)

        self.assertEqual(len(gen.accounts), 100)
        self.assertEqual(len(gen.cards), 200)
        self.assertEqual(len(gen.merchants), 50)
        self.assertEqual(len(gen.devices), 50)
        self.assertEqual(gen.accounts[0].account_id, 1)
        self.assertEqual(gen.cards[0].account_id, gen.accounts[0].account_id)

    def test_fintech_generator_events_and_fraud_rings(self):
        gen = FinTechDataGenerator(seed=42)
        gen.generate_dimensions(num_accounts=100, num_cards=200, num_merchants=20, num_devices=50)
        events = gen.generate_events(num_events=1000, fraud_ratio=0.05)

        self.assertGreaterEqual(len(events), 1000)
        # Verify fraud rings are injected and tracked in ground_truth_fraud
        self.assertGreater(len(gen.ground_truth_fraud), 0)
        patterns = set(gen.ground_truth_fraud.values())
        self.assertIn("SHARED_DEVICE_RING", patterns)
        self.assertIn("IMPOSSIBLE_TRAVEL", patterns)
        self.assertIn("MERCHANT_COLLUSION", patterns)

        # Verify ledger conversion
        ledger = gen.generate_ledger_records(events)
        self.assertGreater(len(ledger), 0)
        self.assertTrue(all(rec.txn_state == "AUTH" for rec in ledger))


if __name__ == "__main__":
    unittest.main()
