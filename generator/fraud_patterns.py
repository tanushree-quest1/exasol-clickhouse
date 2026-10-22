import hashlib
import json
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Tuple
from generator.models import AuthEventModel


def generate_shared_device_fraud(
    base_time: datetime,
    device_id: int,
    card_ids: List[int],
    merchant_id: int,
) -> List[AuthEventModel]:
    """Pattern 1: Shared Device Ring.
    
    A single suspicious device executes rapid authorizations across 10-30 different cards.
    """
    events = []
    current_time = base_time
    for card_id in card_ids:
        current_time += timedelta(seconds=random.randint(2, 25))
        payload = json.dumps({
            "iso8583": {
                "mti": "0100",
                "pan_masked": f"4111********{card_id % 10000:04d}",
                "pos_condition": "08",
                "device_risk_flag": True,
            },
            "network": "VISA",
            "fraud_type": "SHARED_DEVICE",
        })
        event = AuthEventModel(
            event_id=str(uuid.uuid4()),
            event_time=current_time,
            event_date=current_time.date(),
            card_id=card_id,
            merchant_id=merchant_id,
            device_id=device_id,
            amount=Decimal(f"{random.uniform(50.0, 450.0):.2f}"),
            currency="USD",
            auth_status="APPROVED",
            decline_reason="NONE",
            pos_entry_mode="CNP",
            raw_payload=payload,
            is_fraud=1,
            fraud_pattern="SHARED_DEVICE_RING",
        )
        events.append(event)
    return events


def generate_impossible_travel_fraud(
    base_time: datetime,
    card_id: int,
    device_us: int,
    device_remote: int,
    merchant_us: int,
    merchant_remote: int,
) -> List[AuthEventModel]:
    """Pattern 2: Impossible Travel.
    
    The same card is used in New York, and 12 minutes later in Tokyo (physically impossible).
    """
    # Event 1: Legitimate or compromised card used in US
    event1_time = base_time
    payload1 = json.dumps({
        "iso8583": {"mti": "0100", "pos_condition": "00"},
        "geo": {"city": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
        "network": "MASTERCARD",
    })
    event1 = AuthEventModel(
        event_id=str(uuid.uuid4()),
        event_time=event1_time,
        event_date=event1_time.date(),
        card_id=card_id,
        merchant_id=merchant_us,
        device_id=device_us,
        amount=Decimal(f"{random.uniform(15.0, 60.0):.2f}"),
        currency="USD",
        auth_status="APPROVED",
        decline_reason="NONE",
        pos_entry_mode="CHIP",
        raw_payload=payload1,
        is_fraud=0,  # First event appears normal
        fraud_pattern="IMPOSSIBLE_TRAVEL_ORIGIN",
    )

    # Event 2: Used in Tokyo 12 minutes later
    event2_time = event1_time + timedelta(minutes=12)
    payload2 = json.dumps({
        "iso8583": {"mti": "0100", "pos_condition": "01"},
        "geo": {"city": "Tokyo", "country": "JP", "lat": 35.6762, "lon": 139.6503},
        "network": "MASTERCARD",
        "fraud_flag": "VELOCITY_LOCATION_MISMATCH",
    })
    event2 = AuthEventModel(
        event_id=str(uuid.uuid4()),
        event_time=event2_time,
        event_date=event2_time.date(),
        card_id=card_id,
        merchant_id=merchant_remote,
        device_id=device_remote,
        amount=Decimal(f"{random.uniform(500.0, 1500.0):.2f}"),
        currency="USD",
        auth_status="APPROVED",
        decline_reason="NONE",
        pos_entry_mode="CNP",
        raw_payload=payload2,
        is_fraud=1,
        fraud_pattern="IMPOSSIBLE_TRAVEL",
    )

    return [event1, event2]


def generate_merchant_collusion_fraud(
    base_time: datetime,
    rogue_merchant_id: int,
    card_ids: List[int],
    device_id: int,
) -> List[AuthEventModel]:
    """Pattern 3: Merchant Micro-Collusion.
    
    A rogue merchant runs rapid micro-charges ($9.99, $4.99) across compromised cards
    to test cards or drain small amounts without triggering automated alerts.
    """
    events = []
    current_time = base_time
    for card_id in card_ids:
        current_time += timedelta(seconds=random.randint(1, 10))
        payload = json.dumps({
            "iso8583": {"mti": "0100", "pos_condition": "05"},
            "micro_auth": True,
            "merchant_flag": "HIGH_VELOCITY",
        })
        event = AuthEventModel(
            event_id=str(uuid.uuid4()),
            event_time=current_time,
            event_date=current_time.date(),
            card_id=card_id,
            merchant_id=rogue_merchant_id,
            device_id=device_id,
            amount=Decimal(random.choice(["4.99", "9.99", "1.99", "8.99"])),
            currency="USD",
            auth_status="APPROVED",
            decline_reason="NONE",
            pos_entry_mode="CNP",
            raw_payload=payload,
            is_fraud=1,
            fraud_pattern="MERCHANT_COLLUSION",
        )
        events.append(event)
    return events
