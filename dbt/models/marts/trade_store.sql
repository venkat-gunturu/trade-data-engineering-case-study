{{ config
    (
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trade_id',
        post_hook=["update {{ this }} set set status = 'EXPIRED' where maturity_date < current_date and status != 'EXPIRED'"]
    ) 
}}
with source_data as (

    select 
    *
    from {{ ref('int_trade_validation') }}

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
    coalesce(s.trade_id, e.trade_id) as trade_id,
    s.trade_version,
    coalesce(e.trade_type, s.trade_type) as trade_type,
    coalesce(e.instrument_type, s.instrument_type) as instrument_type,
    coalesce(e.counterparty, s.counterparty) as counterparty,
    coalesce(e.trade_date, s.trade_date) as trade_date,
    coalesce(e.event_timestamp, s.event_timestamp) as event_timestamp,
    coalesce(e.maturity_date, s.maturity_date) as maturity_date,
    coalesce(e.notional_amount, s.notional_amount) as notional_amount,
    coalesce(e.currency, s.currency) as currency,
    coalesce(e.price, s.price) as price,
    coalesce(e.quantity, s.quantity) as quantity,
    case when coalesce(e.maturity_date, s.maturity_date) < current_date then 'Expired' else coalesce(e.status, 'VALID') end as status,
    coalesce(e.updated_timestamp, current_timestamp()) as updated_timestamp
from source_data s
left join existing_trades e
on s.trade_id = e.trade_id
where e.trade_id is null
or s.trade_version >= e.trade_version
