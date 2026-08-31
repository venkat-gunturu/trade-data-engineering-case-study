{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}
select
    s.trade_id,
    s.trade_version,
    s.trade_type,
    s.instrument_type,
    s.counterparty,
    s.trade_date,
    s.event_timestamp,
    s.maturity_date,
    s.notional_amount,
    s.currency,
    s.price,
    s.quantity,
    'LOWER_VERSION' as rejection_reason,
    s.processed_at    as rejected_at,
    s.source_file_name,
    s.ingested_at,
    s.batch_id
from {{ ref('int_trade_validation') }} s
join {{ ref('trade_store') }} t
    on s.trade_id = t.trade_id
where s.trade_version < t.trade_version

{% if is_incremental() %}

and not exists (
    select 1
    from {{ this }} r
    where r.trade_id = s.trade_id
      and r.trade_version = s.trade_version
)

{% endif %}