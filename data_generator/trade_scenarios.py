"""Synthetic trade generation with deterministic business-rule scenarios.

Two independent layers:

    base trade builder (Faker + seeded RNG)  ->  decides ATTRIBUTES (realism)
    scenario plan (fixed quotas)             ->  decides BUSINESS CASES (correctness)

The scenario layer is deliberately NOT random. Every mandatory rule case must be
present in every generated data set, so quotas are fixed and the trades they act
on are drawn from state carried across batches:

    history  trade_id -> highest version emitted so far
    catalog  trade_id -> latest attributes, so an amendment keeps its identity
    eligible trade_ids that reached the trade store and can take a new version

That cross-batch state is what makes the scenarios possible at all: a lower-version
trade in BATCH_003 can only exist because BATCH_002 bumped that trade to version 2.

All dates derive from an anchor date (`as_of_date`, defaulting to today), so the
generated data never goes stale and tests can pin the anchor for reproducibility.
"""

import random
from datetime import date, datetime, time, timedelta

from faker import Faker


TRADE_TYPES = ("BUY", "SELL")
INSTRUMENT_TYPES = ("FX", "EQUITY", "BOND", "DERIVATIVE", "COMMODITY")
CURRENCIES = ("USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD")

# The payload contract agreed in README section 4. Ingestion stores this
# unflattened in RAW.RAW_TRADES.payload, so the key set must not drift.
TRADE_FIELDS = (
    "trade_id",
    "trade_version",
    "trade_type",
    "instrument_type",
    "counterparty",
    "trade_date",
    "event_timestamp",
    "maturity_date",
    "notional_amount",
    "currency",
    "price",
    "quantity",
    "trade_status",
)

BATCH_IDS = ("BATCH_001", "BATCH_002", "BATCH_003", "BATCH_004")

# Fixed quotas per batch. Whatever is left over is padded with brand-new trades.
SCENARIO_PLAN = {
    "BATCH_001": {},  # baseline population: all new, all future maturity
    "BATCH_002": {"higher_version": 15, "same_version": 15, "past_maturity": 5},
    "BATCH_003": {"lower_version": 15, "same_version": 10, "past_maturity": 10},
    "BATCH_004": {"higher_version": 20, "same_version": 5, "past_maturity": 5},
}

DEFAULT_RECORDS_PER_BATCH = 200
DEFAULT_SEED = 42

FIRST_TRADE_NUMBER = 100001


def _new_state(seed, as_of_date):
    """Everything one generation run needs to stay reproducible and consistent."""
    fake = Faker()
    fake.seed_instance(seed)
    return {
        "rng": random.Random(seed),
        "fake": fake,
        "as_of": as_of_date,
        "next_number": FIRST_TRADE_NUMBER,
        "history": {},
        "catalog": {},
        "eligible": set(),
    }


def _remember(state, trade, eligible=True):
    """Record a trade in the cross-batch state.

    A lower-version trade must not regress what we believe the store holds, so
    history and catalog only move forward.
    """
    trade_id = trade["trade_id"]
    version = trade["trade_version"]

    if version >= state["history"].get(trade_id, 0):
        state["history"][trade_id] = version
        state["catalog"][trade_id] = dict(trade)

    if eligible:
        state["eligible"].add(trade_id)


def base_trade(state, trade_id, version, maturity_date, trade_status="NEW"):
    """Build one realistic trade. Faker supplies the counterparty, the seeded RNG the rest."""
    rng = state["rng"]

    trade_date = state["as_of"] - timedelta(days=rng.randint(0, 5))
    event_timestamp = datetime.combine(
        trade_date,
        time(rng.randint(8, 17), rng.randint(0, 59), rng.randint(0, 59)),
    )

    return {
        "trade_id": trade_id,
        "trade_version": version,
        "trade_type": rng.choice(TRADE_TYPES),
        "instrument_type": rng.choice(INSTRUMENT_TYPES),
        "counterparty": state["fake"].company(),
        "trade_date": trade_date.isoformat(),
        "event_timestamp": event_timestamp.isoformat(),
        "maturity_date": maturity_date.isoformat(),
        "notional_amount": round(rng.uniform(100_000, 50_000_000), 2),
        "currency": rng.choice(CURRENCIES),
        "price": round(rng.uniform(0.5, 500), 6),
        "quantity": rng.randint(1_000, 5_000_000),
        "trade_status": trade_status,
    }


def _resend(state, trade_id, version, trade_status, mutate_economics):
    """Re-emit an existing trade, keeping its identity but moving it forward in time."""
    rng = state["rng"]
    trade = dict(state["catalog"][trade_id])

    trade["trade_version"] = version
    trade["trade_status"] = trade_status

    # A re-transmission always arrives after the message it supersedes.
    event_timestamp = datetime.fromisoformat(trade["event_timestamp"]) + timedelta(
        minutes=rng.randint(1, 720)
    )
    trade["event_timestamp"] = event_timestamp.isoformat()

    if mutate_economics:
        trade["price"] = round(rng.uniform(0.5, 500), 6)
        trade["notional_amount"] = round(rng.uniform(100_000, 50_000_000), 2)

    return trade


# --- Scenario builders -----------------------------------------------------


def new_trade(state, maturity_date=None):
    """A trade the store has never seen. Version 1, future maturity by default."""
    trade_id = f"T{state['next_number']}"
    state["next_number"] += 1

    if maturity_date is None:
        maturity_date = state["as_of"] + timedelta(days=state["rng"].randint(30, 730))

    trade = base_trade(state, trade_id, 1, maturity_date)
    _remember(state, trade)
    return trade


def past_maturity_trade(state):
    """Rule 3: maturity date earlier than today, so it must be rejected on arrival.

    Not marked eligible: it never reaches the trade store, so later batches must
    not try to version it.
    """
    maturity_date = state["as_of"] - timedelta(days=state["rng"].randint(1, 365))

    trade_id = f"T{state['next_number']}"
    state["next_number"] += 1

    trade = base_trade(state, trade_id, 1, maturity_date)
    _remember(state, trade, eligible=False)
    return trade


def higher_version(state, trade_id):
    """A genuine amendment. Must be accepted and replace the current record."""
    trade = _resend(
        state,
        trade_id,
        state["history"][trade_id] + 1,
        trade_status="AMENDED",
        mutate_economics=True,
    )
    _remember(state, trade)
    return trade


def same_version(state, trade_id):
    """Rule 2: same id and version, changed economics.

    The economics are mutated deliberately. An identical re-send would make
    'replaced', 'ignored', and 'rejected' produce the same trade store contents,
    so the replace path would be untestable. With a changed price the outcome is
    observable: the stored row must show the new price at the same version.
    """
    trade = _resend(
        state,
        trade_id,
        state["history"][trade_id],
        trade_status="AMENDED",
        mutate_economics=True,
    )
    _remember(state, trade)
    return trade


def lower_version(state, trade_id):
    """Rule 1: a stale message arriving late. Must be rejected, store keeps the newer version."""
    trade = _resend(
        state,
        trade_id,
        state["history"][trade_id] - 1,
        trade_status="NEW",
        mutate_economics=False,
    )
    _remember(state, trade)
    return trade


# Builder, plus the minimum current version a trade needs to qualify.
# Going lower requires the trade to already sit at version 2 or above.
SCENARIO_BUILDERS = {
    "lower_version": (lower_version, 2),
    "higher_version": (higher_version, 1),
    "same_version": (same_version, 1),
}

# Most constrained scenario first, so it is not starved of candidates by the others.
SCENARIO_ORDER = ("lower_version", "higher_version", "same_version")


def _pick(state, count, used, min_version):
    """Choose `count` distinct eligible trade ids not already used in this batch."""
    candidates = sorted(
        trade_id
        for trade_id in state["eligible"]
        if trade_id not in used and state["history"][trade_id] >= min_version
    )

    if len(candidates) < count:
        raise ValueError(
            f"Only {len(candidates)} eligible trade(s) at version >= {min_version}, "
            f"need {count}. Increase --records-per-batch or lower the scenario quota."
        )

    picked = state["rng"].sample(candidates, count)
    used.update(picked)
    return picked


def build_batch(state, batch_id, records_per_batch):
    """Fill the batch's scenario quota, then pad with new trades."""
    plan = SCENARIO_PLAN.get(batch_id, {})

    quota = sum(plan.values())
    if quota > records_per_batch:
        raise ValueError(
            f"{batch_id} requires {quota} scenario records but records_per_batch is "
            f"{records_per_batch}. Increase --records-per-batch."
        )

    trades = []
    used = set()

    for name in SCENARIO_ORDER:
        count = plan.get(name, 0)
        if not count:
            continue
        builder, min_version = SCENARIO_BUILDERS[name]
        for trade_id in _pick(state, count, used, min_version):
            trades.append(builder(state, trade_id))

    for _ in range(plan.get("past_maturity", 0)):
        trades.append(past_maturity_trade(state))

    while len(trades) < records_per_batch:
        trades.append(new_trade(state))

    # Scenarios should not sit predictably at the top of the file.
    state["rng"].shuffle(trades)
    return trades


def build_batches(
    records_per_batch=DEFAULT_RECORDS_PER_BATCH,
    seed=DEFAULT_SEED,
    as_of_date=None,
):
    """Build every batch in order, carrying trade state between them.

    Returns {batch_id: [trade, ...]}.
    """
    if records_per_batch < 1:
        raise ValueError("records_per_batch must be at least 1")

    state = _new_state(seed, as_of_date or date.today())

    return {
        batch_id: build_batch(state, batch_id, records_per_batch)
        for batch_id in BATCH_IDS
    }
