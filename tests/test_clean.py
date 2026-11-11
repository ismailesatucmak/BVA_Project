from datetime import datetime

import pytest

from src import clean, quality

SCHEMA = (
    "VendorID int, tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz, "
    "passenger_count bigint, trip_distance double, RatecodeID bigint, PULocationID int, DOLocationID int, "
    "payment_type bigint, fare_amount double, total_amount double, cbd_congestion_fee double"
)
BASE = dict(
    VendorID=2, tpep_pickup_datetime=datetime(2026, 2, 3, 9, 0), tpep_dropoff_datetime=datetime(2026, 2, 3, 9, 15),
    passenger_count=1, trip_distance=3.0, RatecodeID=1, PULocationID=100, DOLocationID=200, payment_type=1,
    fare_amount=15.0, total_amount=22.0, cbd_congestion_fee=0.75,
)
COLUMNS = list(BASE)


def row(**changes):
    values = {**BASE, **changes}
    return tuple(values[c] for c in COLUMNS)


@pytest.fixture(scope="module")
def sample(spark):
    rows = [
        row(),                                                    # 0 geçerli
        row(PULocationID=101),                                    # 1 geçerli
        row(PULocationID=101),                                    # 2 = 1'in kopyası → R9
        row(passenger_count=0, PULocationID=102),                 # 3 R1
        row(trip_distance=0.0, fare_amount=-5.0, PULocationID=103),  # 4 R2 + R3 (örtüşme)
        row(RatecodeID=99, PULocationID=104),                     # 5 R7
        row(payment_type=0, passenger_count=None, RatecodeID=None, PULocationID=105),  # 6 Flex Fare → kalır
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    return clean.flag_rules(quality.add_derived(df)).cache()


def test_flags_mark_expected_rules(sample):
    by_loc = {r.PULocationID: r for r in sample.collect() if r.PULocationID != 101}
    assert by_loc[102].R1 and not by_loc[102].R2
    assert by_loc[103].R2 and by_loc[103].R3
    assert by_loc[104].R7
    flex = by_loc[105]
    assert not any(flex[r] for r in clean.RULE_IDS)


def test_duplicate_rule_keeps_exactly_one_copy(sample):
    dup_rows = [r for r in sample.collect() if r.PULocationID == 101]
    assert sorted(r.R9 for r in dup_rows) == [False, True]


def test_impact_counts_isolated_sequential_and_unique(sample):
    impact = clean.rule_impact(sample).set_index("rule")
    assert impact.loc["R2", "isolated"] == 1
    assert impact.loc["R3", "isolated"] == 1
    assert impact.loc["R2", "sequential"] == 1  # R2 önce gelir, kaydı o siler
    assert impact.loc["R3", "sequential"] == 0  # aynı kaydı R2 zaten silmişti
    assert impact.loc["R2", "unique"] == 0      # R3 de yakaladığı için "yalnız R2" değil
    assert impact.loc[list(clean.RULE_IDS), "sequential"].sum() == 4  # 3, 4, 5 ve kopya 2
    assert impact.loc["TOTAL", "sequential"] == 4


def test_clean_output_keeps_valid_and_flex_rows(sample):
    kept = clean.select_clean(sample)
    assert sorted(r.PULocationID for r in kept.collect()) == [100, 101, 105]
    assert set(clean.RULE_IDS).isdisjoint(kept.columns)
    assert {"pickup_year", "pickup_month", "is_flex_fare", "trip_duration_min"} <= set(kept.columns)


def test_naive_dropna_counts_flex_rows(sample):
    # naif temizlik: herhangi bir null içeren satırı sil → Flex Fare satırı da gider
    assert clean.naive_dropna_removed(sample.select(*COLUMNS)) == 1
