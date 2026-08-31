with TRADE_VALIDATION as (
    select
        *,
        case
            when maturity_date < ingested_at::date
                then 'PAST_MATURITY'
            when prior_version is not null
                then 'LOWER_VERSION'
        end as rejection_reason
    from {{ ref('int_trade_sequence') }}
)
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
    trade_status,
    case
        when rejection_reason is null then 'ACCEPTED'
        else 'REJECTED'
    end as validation_status,
    rejection_reason,
    surrogate_key,
    batch_id,
    source_file_name,
    ingested_at,
    current_timestamp()::timestamp_ntz as processed_at
from TRADE_VALIDATION
