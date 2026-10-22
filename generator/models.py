from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional
import json
import uuid


@dataclass
class AccountModel:
    account_id: int
    customer_id: int
    account_type: str = "CHECKING"
    currency: str = "USD"
    credit_limit: Decimal = Decimal("5000.00")
    current_balance: Decimal = Decimal("1000.00")
    status: str = "ACTIVE"
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class CardModel:
    card_id: int
    account_id: int
    card_number_hash: str
    card_type: str = "VISA_DEBIT"
    expiry_date: date = field(default_factory=lambda: date(2028, 12, 31))
    status: str = "ACTIVE"
    is_contactless: int = 1
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class MerchantModel:
    merchant_id: int
    merchant_name: str
    mcc: int = 5411  # Grocery Stores
    risk_tier: str = "LOW"
    city: str = "New York"
    country: str = "US"
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DeviceModel:
    device_id: int
    device_fingerprint: str
    device_type: str = "MOBILE_IOS"
    os: str = "iOS 17"
    ip_address: str = "192.168.1.1"
    first_seen_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AuthEventModel:
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_time: datetime = field(default_factory=datetime.utcnow)
    event_date: date = field(default_factory=lambda: datetime.utcnow().date())
    card_id: int = 0
    merchant_id: int = 0
    device_id: int = 0
    amount: Decimal = Decimal("0.00")
    currency: str = "USD"
    auth_status: str = "APPROVED"
    decline_reason: str = "NONE"
    pos_entry_mode: str = "CONTACTLESS"
    raw_payload: str = "{}"
    is_fraud: int = 0
    fraud_pattern: str = "NONE"

    def to_ch_row(self) -> tuple:
        """Tuple format matching ClickHouse schema."""
        return (
            uuid.UUID(self.event_id),
            self.event_time,
            self.event_date,
            self.card_id,
            self.merchant_id,
            self.device_id,
            self.amount,
            self.currency,
            self.auth_status,
            self.decline_reason,
            self.pos_entry_mode,
            self.raw_payload,
        )

    def to_exasol_row(self) -> tuple:
        """Tuple format matching Exasol schema."""
        return (
            self.event_id,
            self.event_time,
            self.event_date,
            self.card_id,
            self.merchant_id,
            self.device_id,
            self.amount,
            self.currency,
            self.auth_status,
            self.decline_reason,
            self.pos_entry_mode,
            self.raw_payload,
        )


@dataclass
class TxnLedgerModel:
    txn_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    account_id: int = 0
    card_id: int = 0
    amount: Decimal = Decimal("0.00")
    txn_state: str = "AUTH"  # AUTH, CLEARED, SETTLED, REVERSED, CHARGEBACK
    settlement_date: Optional[date] = None
    version: int = 1
    updated_at: datetime = field(default_factory=datetime.utcnow)
