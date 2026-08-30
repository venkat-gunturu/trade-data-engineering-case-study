{#
    SILVER, step 1 of 2: reconstruct arrival order.

    RAW is append-only and these models are full rebuilds, so "the trade as it
    stood when this record arrived" has to be derived rather than read from
    current state. This model does that derivation and nothing else. No business
    rules are applied here.

    Arrival order is (event_timestamp, surrogate_key): event_timestamp is the
    source system's own sequencing, surrogate_key is the append-only tie-break.

    Adds two columns, both null for the first record of a trade:

      prior_version           highest version seen for this trade before this record
      previously_established  1 when an earlier record had already reached the
                              trade store, i.e. was not past maturity on arrival
#}

select
    *,

    max(trade_version) over (
        partition by trade_id
        order by event_timestamp, surrogate_key
        rows between unbounded preceding and 1 preceding
    ) as prior_version,

    max(case when maturity_date >= ingested_at::date then 1 else 0 end) over (
        partition by trade_id
        order by event_timestamp, surrogate_key
        rows between unbounded preceding and 1 preceding
    ) as previously_established

from {{ ref('stg_trades') }}
