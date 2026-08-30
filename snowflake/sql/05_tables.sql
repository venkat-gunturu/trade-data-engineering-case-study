CREATE TABLE IF NOT EXISTS TRADE_DB.RAW.RAW_TRADES (
    surrogate_key      NUMBER AUTOINCREMENT,
    payload            VARIANT NOT NULL,
    batch_id           VARCHAR(100) NOT NULL,
    source_file_name   VARCHAR(255) NOT NULL,
    ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS TRADE_DB.BRONZE.STG_TRADES (
    trade_id            VARCHAR(50),
    trade_version       NUMBER(10,0),
    trade_type          VARCHAR(30),
    instrument_type     VARCHAR(30),
    counterparty        VARCHAR(100),
    trade_date          DATE,
    event_timestamp     TIMESTAMP_NTZ,
    maturity_date       DATE,
    notional_amount     NUMBER(18,2),
    currency            VARCHAR(3),
    price               NUMBER(18,6),
    quantity            NUMBER(18,6),
    trade_status        VARCHAR(30),
    surrogate_key       NUMBER,
    batch_id            VARCHAR(100),
    source_file_name    VARCHAR(255),
    ingested_at         TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS TRADE_DB.SILVER.INT_TRADE_VALIDATION (
    trade_id            VARCHAR(50),
    trade_version       NUMBER(10,0),
    trade_type          VARCHAR(30),
    instrument_type     VARCHAR(30),
    counterparty        VARCHAR(100),
    trade_date          DATE,
    event_timestamp     TIMESTAMP_NTZ,
    maturity_date       DATE,
    notional_amount     NUMBER(18,2),
    currency            VARCHAR(3),
    price               NUMBER(18,6),
    quantity            NUMBER(18,6),
    trade_status        VARCHAR(30),
-----------------------------------------------------------------
    validation_status   VARCHAR(20),
    rejection_reason    VARCHAR(100),
-----------------------------------------------------------------
    surrogate_key       NUMBER,
    batch_id            VARCHAR(100),
    source_file_name    VARCHAR(255),
    ingested_at         TIMESTAMP_NTZ,
    processed_at        TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS TRADE_DB.GOLD.TRADE_STORE (
    trade_id            VARCHAR(50),
    trade_version       NUMBER(10,0),
    trade_type          VARCHAR(30),
    instrument_type     VARCHAR(30),
    counterparty        VARCHAR(100),
    trade_date          DATE,
    event_timestamp     TIMESTAMP_NTZ,
    maturity_date       DATE,
    notional_amount     NUMBER(18,2),
    currency            VARCHAR(3),
    price               NUMBER(18,6),
    quantity            NUMBER(18,6),

    status              VARCHAR(20),
    updated_timestamp   TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS TRADE_DB.GOLD.REJECTED_TRADES (
    trade_id            VARCHAR(50),
    trade_version       NUMBER(10,0),
    trade_type          VARCHAR(30),
    instrument_type     VARCHAR(30),
    counterparty        VARCHAR(100),
    trade_date          DATE,
    event_timestamp     TIMESTAMP_NTZ,
    maturity_date       DATE,
    notional_amount     NUMBER(18,2),
    currency            VARCHAR(3),
    price               NUMBER(18,6),
    quantity            NUMBER(18,6),

    rejection_reason    VARCHAR(100),
    rejected_at         TIMESTAMP_NTZ,

    source_file_name    VARCHAR(255),
    ingested_at         TIMESTAMP_NTZ,
    batch_id            VARCHAR(100)
);