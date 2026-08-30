-- An ACCEPTED record must carry no rejection reason, and a REJECTED record must
-- always carry one. Returns rows only on failure.

select
    surrogate_key,
    trade_id,
    validation_status,
    rejection_reason
from {{ ref('int_trade_validation') }}
where (validation_status = 'ACCEPTED' and rejection_reason is not null)
   or (validation_status = 'REJECTED' and rejection_reason is null)
