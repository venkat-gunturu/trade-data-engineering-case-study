{#
    SILVER, step 2 of 2: classify every trade as ACCEPTED or REJECTED.

    Sequencing is already resolved upstream by int_trade_sequence, so this model
    is only the rule decision.

    Rule 3 - PAST_MATURITY
        The trade was already past its maturity date when it arrived, and no
        earlier record had established it in the store. Checked first: an
        intrinsic property of the record takes precedence over a comparison
        against stored state.

        A trade that was valid when ingested is deliberately NOT rejected here.
        It stays eligible for GOLD.TRADE_STORE, where rule 4 marks it EXPIRED
        once CURRENT_DATE passes its maturity date.

        ingested_at is never in the future, so maturity_date < ingested_at::date
        already implies maturity_date < current_date().

    Rule 1 - LOWER_VERSION
        A stale message whose version is below the highest version already seen
        for this trade. Rejected, and the newer version stays current.

    Nothing is filtered out: every staged trade appears here exactly once, so the
    audit trail stays complete.

    Business-state idempotent: re-running over unchanged RAW yields the same
    classification and the same resulting trade state. Operational timestamps
    such as processed_at differ between runs; the business state does not.
#}

with sequenced as (

    select * from {{ ref('int_trade_sequence') }}

),

classified as (

    select
        *,
        case
            when maturity_date < ingested_at::date
                 and coalesce(previously_established, 0) = 0
                then 'PAST_MATURITY'
            when prior_version is not null
                 and trade_version < prior_version
                then 'LOWER_VERSION'
        end as rejection_reason

    from sequenced

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
    end                                     as validation_status,
    rejection_reason,

    surrogate_key,
    batch_id,
    source_file_name,
    ingested_at,
    current_timestamp()::timestamp_ntz      as processed_at

from classified
