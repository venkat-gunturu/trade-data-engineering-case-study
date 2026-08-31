{{ config
    (
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trade_id',
        post_hook=["update {{ this }} set status = 'EXPIRED' where maturity_date < current_date and status != 'EXPIRED'"]
    ) 
}}
with source_data as (

    select 
    *
    from {{ ref('int_trade_validation') }}
    where validation_status = 'ACCEPTED'

),
existing_trades as (

    {% if is_incremental() %}

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
        status,
        updated_timestamp
    from {{ this }}
    where status = 'VALID'
    {% else %}
    select
        cast(null as varchar) as trade_id,
        cast(null as number) as trade_version,
        cast(null as varchar) as trade_type,
        cast(null as varchar) as instrument_type,
        cast(null as varchar) as counterparty,
        cast(null as date) as trade_date,
        cast(null as timestamp) as event_timestamp,
        cast(null as date) as maturity_date,
        cast(null as number) as notional_amount,
        cast(null as varchar) as currency,
        cast(null as number) as price,
        cast(null as number) as quantity,
        cast(null as varchar) as status,
        cast(null as timestamp) as updated_timestamp
    where false
    {% endif %}
)
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
    case when coalesce(e.maturity_date, s.maturity_date) < current_date then 'EXPIRED' else coalesce(e.status, 'VALID') end as status,
    current_timestamp() as updated_timestamp
from source_data s
left join existing_trades e
on s.trade_id = e.trade_id
where e.trade_id is null
or s.trade_version >= e.trade_version
