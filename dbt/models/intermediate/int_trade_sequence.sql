select
    *,
    lead(trade_version) over (
        partition by trade_id
        order by event_timestamp, surrogate_key
        ) as prior_version,
    max(case when maturity_date >= ingested_at::date then 1 else 0 end) over (
        partition by trade_id
        order by event_timestamp, surrogate_key
        rows between unbounded preceding and 1 preceding
    ) as previously_established
from {{ ref('stg_trades') }}
