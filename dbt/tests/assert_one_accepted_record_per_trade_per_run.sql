-- The invariant the GOLD merge depends on. Returns rows only on failure.
--
-- int_trade_sequence rejects every arrival that a later one supersedes, so a run
-- must leave at most one ACCEPTED record per trade_id. trade_store does no
-- ranking or deduplication of its own, so if this ever breaks the MERGE either
-- inserts a duplicate trade_id or fails outright on a nondeterministic match.

select
    trade_id,
    count(*) as accepted_records
from {{ ref('int_trade_validation') }}
where validation_status = 'ACCEPTED'
group by trade_id
having count(*) > 1
