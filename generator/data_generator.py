import hashlib
import json
import random
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Iterator, List, Tuple
from generator.models import (
    AccountModel,
    CardModel,
    MerchantModel,
    DeviceModel,
    AuthEventModel,
    TxnLedgerModel,
)
from generator.fraud_patterns import (
    generate_shared_device_fraud,
    generate_impossible_travel_fraud,
    generate_merchant_collusion_fraud,
)


class FinTechDataGenerator:
    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.accounts: List[AccountModel] = []
        self.cards: List[CardModel] = []
        self.merchants: List[MerchantModel] = []
        self.devices: List[DeviceModel] = []
        self.ground_truth_fraud: Dict[str, str] = {}  # event_id -> fraud_pattern

    def generate_dimensions(
        self,
        num_accounts: int = 1000,
        num_cards: int = 2000,
        num_merchants: int = 200,
        num_devices: int = 500,
    ):
        """Generates dimension records."""
        base_time = datetime(2025, 1, 1, 0, 0, 0)

        # 1. Accounts
        self.accounts = []
        for i in range(1, num_accounts + 1):
            created_at = base_time + timedelta(days=random.randint(0, 180))
            self.accounts.append(
                AccountModel(
                    account_id=i,
                    customer_id=100000 + i,
                    account_type=random.choice(["CHECKING", "SAVINGS", "CREDIT"]),
                    currency="USD",
                    credit_limit=Decimal(random.choice([2500, 5000, 10000, 25000])),
                    current_balance=Decimal(f"{random.uniform(200.0, 4000.0):.2f}"),
                    status="ACTIVE",
                    created_at=created_at,
                )
            )

        # 2. Cards (linked to accounts)
        self.cards = []
        for i in range(1, num_cards + 1):
            acct = self.accounts[(i - 1) % num_accounts]
            card_hash = hashlib.md5(f"CARD_{i}_{acct.account_id}".encode()).hexdigest()
            created_at = acct.created_at + timedelta(days=random.randint(1, 30))
            self.cards.append(
                CardModel(
                    card_id=i,
                    account_id=acct.account_id,
                    card_number_hash=card_hash,
                    card_type=random.choice(["VISA_DEBIT", "MASTERCARD_CREDIT", "AMEX"]),
                    expiry_date=date(2028, (i % 12) + 1, 28),
                    status="ACTIVE",
                    is_contactless=1 if random.random() > 0.1 else 0,
                    created_at=created_at,
                )
            )

        # 3. Merchants
        self.merchants = []
        cities_countries = [
            ("New York", "US"),
            ("San Francisco", "US"),
            ("Chicago", "US"),
            ("London", "GB"),
            ("Tokyo", "JP"),
            ("Frankfurt", "DE"),
            ("Mumbai", "IN"),
        ]
        mccs = [5411, 5812, 5999, 4814, 5541, 7011]  # Grocery, Restaurant, Misc, Telecom, Gas, Hotel
        for i in range(1, num_merchants + 1):
            city, country = random.choice(cities_countries)
            self.merchants.append(
                MerchantModel(
                    merchant_id=i,
                    merchant_name=f"Merchant_{i}_{city}",
                    mcc=random.choice(mccs),
                    risk_tier="HIGH" if i == 13 else random.choice(["LOW", "LOW", "MEDIUM"]),
                    city=city,
                    country=country,
                    created_at=base_time + timedelta(days=random.randint(0, 90)),
                )
            )

        # 4. Devices
        self.devices = []
        device_types = ["MOBILE_IOS", "MOBILE_ANDROID", "POS_TERMINAL", "WEB_BROWSER"]
        for i in range(1, num_devices + 1):
            self.devices.append(
                DeviceModel(
                    device_id=i,
                    device_fingerprint=hashlib.md5(f"DEV_{i}".encode()).hexdigest(),
                    device_type=random.choice(device_types),
                    os=random.choice(["iOS 17", "Android 14", "Windows 11", "macOS 14"]),
                    ip_address=f"10.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}",
                    first_seen_at=base_time + timedelta(days=random.randint(0, 90)),
                )
            )

    def generate_events(
        self,
        num_events: int = 10000,
        start_time: datetime = datetime(2025, 7, 1, 8, 0, 0),
        fraud_ratio: float = 0.02,
    ) -> List[AuthEventModel]:
        """Generates authentic stream of auth_events with injected fraud rings."""
        if not self.cards:
            self.generate_dimensions()

        events: List[AuthEventModel] = []
        current_time = start_time
        # 1. Generate normal transactions
        for _ in range(num_events):
            current_time += timedelta(milliseconds=random.randint(20, 200))
            card = random.choice(self.cards)
            merchant = random.choice(self.merchants)
            device = random.choice(self.devices)

            amount = Decimal(f"{random.uniform(2.50, 250.0):.2f}")
            status = "APPROVED" if random.random() > 0.05 else "DECLINED"
            decline_reason = "NONE" if status == "APPROVED" else random.choice(["INSUFFICIENT_FUNDS", "DO_NOT_HONOR"])

            payload = json.dumps({
                "iso8583": {
                    "mti": "0100",
                    "pan_masked": f"4111********{card.card_id % 10000:04d}",
                    "pos_condition": "00",
                },
                "network": card.card_type.split("_")[0],
                "merchant_category": merchant.mcc,
            })

            event = AuthEventModel(
                event_id=str(uuid.uuid4()),
                event_time=current_time,
                event_date=current_time.date(),
                card_id=card.card_id,
                merchant_id=merchant.merchant_id,
                device_id=device.device_id,
                amount=amount,
                currency="USD",
                auth_status=status,
                decline_reason=decline_reason,
                pos_entry_mode=random.choice(["CONTACTLESS", "CHIP", "CNP"]),
                raw_payload=payload,
                is_fraud=0,
                fraud_pattern="NONE",
            )
            events.append(event)

        # 2. Inject Fraud Ring 1: Shared Device Ring
        fraud_device = self.devices[0].device_id
        fraud_cards_ring1 = [c.card_id for c in random.sample(self.cards, min(25, len(self.cards)))]
        ring1_events = generate_shared_device_fraud(
            base_time=start_time + timedelta(hours=2),
            device_id=fraud_device,
            card_ids=fraud_cards_ring1,
            merchant_id=self.merchants[0].merchant_id,
        )
        for e in ring1_events:
            self.ground_truth_fraud[e.event_id] = e.fraud_pattern
        events.extend(ring1_events)

        # 3. Inject Fraud Ring 2: Impossible Travel
        victim_card = self.cards[1].card_id
        us_merch = next(m.merchant_id for m in self.merchants if m.country == "US")
        jp_merch = next(m.merchant_id for m in self.merchants if m.country == "JP")
        travel_events = generate_impossible_travel_fraud(
            base_time=start_time + timedelta(hours=3),
            card_id=victim_card,
            device_us=self.devices[1].device_id,
            device_remote=self.devices[2].device_id,
            merchant_us=us_merch,
            merchant_remote=jp_merch,
        )
        for e in travel_events:
            if e.is_fraud:
                self.ground_truth_fraud[e.event_id] = e.fraud_pattern
        events.extend(travel_events)

        # 4. Inject Fraud Ring 3: Merchant Micro-Collusion
        rogue_merchant = self.merchants[0].merchant_id
        collusion_cards = [c.card_id for c in random.sample(self.cards, min(20, len(self.cards)))]
        collusion_events = generate_merchant_collusion_fraud(
            base_time=start_time + timedelta(hours=4),
            rogue_merchant_id=rogue_merchant,
            card_ids=collusion_cards,
            device_id=self.devices[3].device_id,
        )
        for e in collusion_events:
            self.ground_truth_fraud[e.event_id] = e.fraud_pattern
        events.extend(collusion_events)

        # Sort all events chronologically
        events.sort(key=lambda x: x.event_time)
        return events

    def generate_ledger_records(self, auth_events: List[AuthEventModel]) -> List[TxnLedgerModel]:
        """Generates initial AUTH state ledger records from approved auth events."""
        card_to_account = {c.card_id: c.account_id for c in self.cards}
        ledger_records = []
        for e in auth_events:
            if e.auth_status == "APPROVED":
                acct_id = card_to_account.get(e.card_id, 1)
                ledger_records.append(
                    TxnLedgerModel(
                        txn_id=e.event_id,
                        account_id=acct_id,
                        card_id=e.card_id,
                        amount=e.amount,
                        txn_state="AUTH",
                        settlement_date=None,
                        version=1,
                        updated_at=e.event_time,
                    )
                )
        return ledger_records
