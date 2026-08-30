{#
    BRONZE: extract the trade attributes from the RAW VARIANT payload and cast
    them to the agreed relational types.

    Extraction and casting only - no business logic. Exactly one output row per
    RAW row, so surrogate_key stays unique here and downstream.
#}

select
    payload:trade_id::varchar(50)            as trade_id,
    payload:trade_version::number(10,0)      as trade_version,
    payload:trade_type::varchar(30)          as trade_type,
    payload:instrument_type::varchar(30)     as instrument_type,
    payload:counterparty::varchar(100)       as counterparty,
    payload:trade_date::date                 as trade_date,
    payload:event_timestamp::timestamp_ntz   as event_timestamp,
    payload:maturity_date::date              as maturity_date,
    payload:notional_amount::number(18,2)    as notional_amount,
    payload:currency::varchar(3)             as currency,
    payload:price::number(18,6)              as price,
    payload:quantity::number(18,6)           as quantity,
    payload:trade_status::varchar(30)        as trade_status,

    -- ingestion metadata, carried through unchanged
    surrogate_key,
    batch_id,
    source_file_name,
    ingested_at

from {{ source('raw', 'raw_trades') }}
