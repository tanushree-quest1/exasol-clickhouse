-- =====================================================================
-- ClickHouse DDL for PayLens FinTech Workload
-- Demonstrates:
--   1. Partitioning by month: PARTITION BY toYYYYMM(event_time)
--   2. Explicit sparse primary key: ORDER BY (merchant_id, event_time, card_id)
--   3. Data skipping index: Bloom filter on card_id
--   4. Declarative TTL: Automated row deletion after 90 days
--   5. ReplacingMergeTree: Idiomatic ClickHouse mutable ledger state
-- =====================================================================

CREATE DATABASE IF NOT EXISTS paylens;

-- ---------------------------------------------------------------------
-- 1. Dimension Tables
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS paylens.accounts
(
    account_id       UInt64,
    customer_id      UInt64,
    account_type     LowCardinality(String),
    currency         LowCardinality(String),
    credit_limit     Decimal64(2),
    current_balance  Decimal64(2),
    status           LowCardinality(String),
    created_at       DateTime
)
ENGINE = MergeTree()
ORDER BY account_id;

CREATE TABLE IF NOT EXISTS paylens.cards
(
    card_id          UInt64,
    account_id       UInt64,
    card_number_hash FixedString(32),
    card_type        LowCardinality(String),
    expiry_date      Date,
    status           LowCardinality(String),
    is_contactless   UInt8,
    created_at       DateTime
)
ENGINE = MergeTree()
ORDER BY (account_id, card_id);

CREATE TABLE IF NOT EXISTS paylens.merchants
(
    merchant_id      UInt64,
    merchant_name    String,
    mcc              UInt16, -- Merchant Category Code
    risk_tier        LowCardinality(String),
    city             String,
    country          LowCardinality(FixedString(2)),
    created_at       DateTime
)
ENGINE = MergeTree()
ORDER BY (country, mcc, merchant_id);

CREATE TABLE IF NOT EXISTS paylens.devices
(
    device_id        UInt64,
    device_fingerprint FixedString(32),
    device_type      LowCardinality(String),
    os               LowCardinality(String),
    ip_address       IPv4,
    first_seen_at    DateTime
)
ENGINE = MergeTree()
ORDER BY (device_id);

-- ---------------------------------------------------------------------
-- 2. Streaming Fact Table: auth_events (Append-only)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS paylens.auth_events
(
    event_id         UUID,
    event_time       DateTime64(3, 'UTC'),
    event_date       Date DEFAULT toDate(event_time),
    card_id          UInt64,
    merchant_id      UInt64,
    device_id        UInt64,
    amount           Decimal64(2),
    currency         LowCardinality(String),
    auth_status      LowCardinality(String), -- APPROVED, DECLINED, BLOCKED
    decline_reason   LowCardinality(String),
    pos_entry_mode   LowCardinality(String), -- CHIP, CONTACTLESS, SWIPE, CNP (Online)
    raw_payload      String,                 -- JSON payload (ISO 8583/20022 fields)
    
    -- Secondary Data Skipping Index: Accelerated lookups by card_id
    INDEX idx_card_bloom card_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (merchant_id, event_time, card_id)
TTL event_time + INTERVAL 90 DAY DELETE;

-- ---------------------------------------------------------------------
-- 3. Mutable Ledger Table: txn_ledger
-- ClickHouse handles mutations via ReplacingMergeTree (deduplication on PK by version)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS paylens.txn_ledger
(
    txn_id           UUID,
    account_id       UInt64,
    card_id          UInt64,
    amount           Decimal64(2),
    txn_state        LowCardinality(String), -- AUTH, CLEARED, SETTLED, REVERSED, CHARGEBACK
    settlement_date  Nullable(Date),
    version          UInt64,                 -- Monotonically increasing state version
    updated_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (account_id, txn_id);

-- ---------------------------------------------------------------------
-- 4. Fraud Scoring Output Table
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS paylens.fraud_scores
(
    event_id         UUID,
    card_id          UInt64,
    scored_at        DateTime64(3, 'UTC'),
    fraud_probability Float32,
    decision         LowCardinality(String), -- APPROVE, REVIEW, BLOCK
    model_version    LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY (scored_at, card_id, event_id);
