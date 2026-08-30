{#
    GOLD: current state of each trade. Exactly one row per trade_id.

    Rule 2 (same version replaces) falls out of the ordering rather than needing
    its own branch: among accepted records the winner is the highest version,
    then the latest event_timestamp, then the highest surrogate_key. A
    same-version re-send always carries a later event_timestamp, so it replaces
    the record it supersedes.

    Rule 1 needs nothing here - lower-version records were already rejected in
    SILVER and never reach this model, so they cannot displace the current record.

    Rule 4 (expiration) is applied here, against CURRENT_DATE, so a stored trade
    flips to EXPIRED as soon as its maturity date passes.

    No SCD2 / history in this phase: superseded versions remain visible in
    SILVER and in RAW, but the store holds current state only.
#}

with accepted as (

    select *
    from {{ ref('int_trade_validation') }}
    where validation_status = 'ACCEPTED'

),

ranked as (

    select
        *,
        row_number() over (
            partition by trade_id
            order by trade_version desc, event_timestamp desc, surrogate_key desc
        ) as current_record_rank
    from accepted

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

    case
        when maturity_date < current_date() then 'EXPIRED'
        else 'VALID'
    end                                  as status,

    current_timestamp()::timestamp_ntz   as updated_timestamp

from ranked
where current_record_rank = 1
