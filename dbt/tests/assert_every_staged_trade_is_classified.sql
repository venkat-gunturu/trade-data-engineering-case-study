-- Row-count integrity between BRONZE and SILVER. Returns rows only on failure.
--
-- int_trade_validation classifies but never filters, so the audit trail stays
-- complete: every staged trade must appear exactly once, whether it was accepted
-- or rejected.
--
-- The GOLD counts this test used to make were removed when trade_store became
-- incremental. They compared a cumulative store against a single run's SILVER
-- output, which are different populations over different time windows and can
-- never be expected to match. The GOLD invariants are asserted by the `unique`
-- test on trade_store.trade_id and by the unit tests in models/marts.

with staged as (
    select count(*) as n from {{ ref('stg_trades') }}
),

validated as (
    select count(*) as n from {{ ref('int_trade_validation') }}
)

select
    'staged_vs_validated' as check_name,
    staged.n              as left_value,
    validated.n           as right_value
from staged, validated
where staged.n != validated.n
