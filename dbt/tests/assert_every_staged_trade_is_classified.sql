-- Row-count integrity across the layers. Returns rows only on failure.
--
--   staged_vs_validated         every staged trade is classified exactly once
--   rejected_vs_audit           every rejected record reaches the audit table
--   store_vs_distinct_accepted  the store holds one row per accepted trade_id

with staged as (
    select count(*) as n from {{ ref('stg_trades') }}
),

validated as (
    select count(*) as n from {{ ref('int_trade_validation') }}
),

accepted as (
    select count(*) as n
    from {{ ref('int_trade_validation') }}
    where validation_status = 'ACCEPTED'
),

audited as (
    select count(*) as n from {{ ref('rejected_trades') }}
),

stored as (
    select count(*) as n from {{ ref('trade_store') }}
),

distinct_accepted as (
    select count(distinct trade_id) as n
    from {{ ref('int_trade_validation') }}
    where validation_status = 'ACCEPTED'
)

select 'staged_vs_validated' as check_name, staged.n as left_value, validated.n as right_value
from staged, validated
where staged.n != validated.n

union all

select 'rejected_vs_audit', validated.n - accepted.n, audited.n
from validated, accepted, audited
where validated.n - accepted.n != audited.n

union all

select 'store_vs_distinct_accepted', stored.n, distinct_accepted.n
from stored, distinct_accepted
where stored.n != distinct_accepted.n
