{#
    GOLD: audit trail for compliance (rule 5).

    Every rejected record is preserved with its original trade attributes, the
    rejection reason, and the ingestion metadata needed to trace it back to the
    source file and batch.

    A trade_id may legitimately appear in BOTH this table and TRADE_STORE: a
    stale lower-version message is rejected here while the newer version remains
    current in the store. That is expected, so no mutual-exclusivity test exists.
#}

select
    trade_id,
    trade_version,
    trade_type,
    instrument_type,
    counterparty,
    trade_date,
    event_timestamp,
    maturity_date,
    notional_amount,
    currency,
    price,
    quantity,

    rejection_reason,
    processed_at    as rejected_at,

    source_file_name,
    ingested_at,
    batch_id

from {{ ref('int_trade_validation') }}
where validation_status = 'REJECTED'
