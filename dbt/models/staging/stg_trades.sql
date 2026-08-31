{{ config(
    post_hook="UPDATE trade_db.monitoring.load_control SET STATUS='COMPLETED' WHERE STATUS='LOADED'"
)}}
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
    surrogate_key,
    batch_id,
    source_file_name,
    ingested_at
from {{ source('raw', 'raw_trades') }}
where source_file_name in (
select filename from trade_db.monitoring.load_control where status='LOADED')