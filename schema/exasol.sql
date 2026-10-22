-- =====================================================================
-- Exasol DDL for PayLens FinTech Workload
-- Demonstrates:
--   1. Distribution keys: DISTRIBUTE BY for co-locating join keys
--   2. Partitioning keys: PARTITION BY for sub-node partition pruning
--   3. Indexing strategy: indexes are created and maintained automatically
--      for supported operations; inspect optimizer/profile evidence per query.
--   4. TTL strategy: NO DECLARATIVE TTL (Managed via partition drop / script)
--   5. Full ACID relational tables for synchronous UPDATE / MERGE INTO
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS PAYLENS;
OPEN SCHEMA PAYLENS;

-- ---------------------------------------------------------------------
-- 1. Dimension Tables
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS accounts
(
    account_id       DECIMAL(18, 0),
    customer_id      DECIMAL(18, 0),
    account_type     VARCHAR(20),
    currency         CHAR(3),
    credit_limit     DECIMAL(18, 2),
    current_balance  DECIMAL(18, 2),
    status           VARCHAR(20),
    created_at       TIMESTAMP,
    CONSTRAINT pk_accounts PRIMARY KEY (account_id),
    DISTRIBUTE BY account_id
);

CREATE TABLE IF NOT EXISTS cards
(
    card_id          DECIMAL(18, 0),
    account_id       DECIMAL(18, 0),
    card_number_hash VARCHAR(32),
    card_type        VARCHAR(20),
    expiry_date      DATE,
    status           VARCHAR(20),
    is_contactless   BOOLEAN,
    created_at       TIMESTAMP,
    CONSTRAINT pk_cards PRIMARY KEY (card_id),
    DISTRIBUTE BY account_id
);

CREATE TABLE IF NOT EXISTS merchants
(
    merchant_id      DECIMAL(18, 0),
    merchant_name    VARCHAR(100),
    mcc              DECIMAL(6, 0),
    risk_tier        VARCHAR(20),
    city             VARCHAR(50),
    country          CHAR(2),
    created_at       TIMESTAMP,
    CONSTRAINT pk_merchants PRIMARY KEY (merchant_id)
);

CREATE TABLE IF NOT EXISTS devices
(
    device_id        DECIMAL(18, 0),
    device_fingerprint VARCHAR(32),
    device_type      VARCHAR(20),
    os               VARCHAR(20),
    ip_address       VARCHAR(45),
    first_seen_at    TIMESTAMP,
    CONSTRAINT pk_devices PRIMARY KEY (device_id)
);

-- ---------------------------------------------------------------------
-- 2. Streaming Fact Table: auth_events (Append-only)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS auth_events
(
    event_id         VARCHAR(36),    -- UUID representation
    event_time       TIMESTAMP,
    event_date       DATE,
    card_id          DECIMAL(18, 0),
    merchant_id      DECIMAL(18, 0),
    device_id        DECIMAL(18, 0),
    amount           DECIMAL(18, 2),
    currency         CHAR(3),
    auth_status      VARCHAR(20),
    decline_reason   VARCHAR(50),
    pos_entry_mode   VARCHAR(20),
    raw_payload      VARCHAR(2000000), -- Exasol VARCHAR up to 2M chars for JSON
    DISTRIBUTE BY card_id,
    PARTITION BY event_date
);

-- NOTE ON INDEXING & RETENTION FOR EXASOL:
-- 1. Exasol automatically creates and maintains indexes for join operations;
--    inspect EXA_ALL_INDICES and query profiling rather than assuming an index
--    exists or was used for every predicate.
-- 2. This benchmark has no Exasol TTL configured. Evaluate the supported
--    partition-management or scheduled deletion procedure separately and
--    record its exact SQL and operational steps for the selected DB version.

-- ---------------------------------------------------------------------
-- 3. Mutable Ledger Table: txn_ledger (Full ACID)
-- Supports native UPDATE, DELETE, and MERGE INTO statements
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS txn_ledger
(
    txn_id           VARCHAR(36),
    account_id       DECIMAL(18, 0),
    card_id          DECIMAL(18, 0),
    amount           DECIMAL(18, 2),
    txn_state        VARCHAR(20),   -- AUTH, CLEARED, SETTLED, REVERSED, CHARGEBACK
    settlement_date  DATE,
    version          DECIMAL(18, 0),
    updated_at       TIMESTAMP,
    CONSTRAINT pk_txn_ledger PRIMARY KEY (account_id, txn_id),
    DISTRIBUTE BY account_id
);

-- ---------------------------------------------------------------------
-- 4. Fraud Scoring Output Table
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_scores
(
    event_id         VARCHAR(36),
    card_id          DECIMAL(18, 0),
    scored_at        TIMESTAMP,
    fraud_probability DOUBLE PRECISION,
    decision         VARCHAR(20),
    model_version    VARCHAR(20),
    DISTRIBUTE BY card_id
);
