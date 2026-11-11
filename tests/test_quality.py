from datetime import datetime

import pytest
from pyspark.sql import functions as F

from src import quality

COLUMNS = (
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
    "RatecodeID", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "total_amount",
    "cbd_congestion_fee",
)
# Geçerli bir referans yolculuk: 12 dk, 2 mil → 10 mph
VALID = (datetime(2026, 1, 10, 8, 0), datetime(2026, 1, 10, 8, 12), 1, 2.0, 1, 100, 200, 1, 10.0, 15.0, 0.75)


def _row(**changes):
    values = dict(zip(COLUMNS, VALID))
    values.update(changes)
    return tuple(values[c] for c in COLUMNS)


@pytest.fixture(scope="module")
def frame(spark):
    def build(*rows):
        schema = (
            "tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz, passenger_count bigint, "
            "trip_distance double, RatecodeID bigint, PULocationID int, DOLocationID int, payment_type bigint, "
            "fare_amount double, total_amount double, cbd_congestion_fee double"
        )
        return quality.add_derived(spark.createDataFrame(list(rows), schema))

    return build


def _flags(df, name):
    return [r[0] for r in df.select(quality.logic_checks()[name].alias("f")).collect()]


def test_add_derived_columns(frame):
    row = frame(_row()).first()
    assert row.trip_duration_min == pytest.approx(12.0)
    assert row.avg_speed_mph == pytest.approx(10.0)
    assert row.pickup_hour == 8
    assert row.pickup_dow == 7  # 10 Ocak 2026 Cumartesi; Spark: 1=Pazar … 7=Cumartesi
    assert row.is_flex_fare is False


def test_speed_is_null_when_duration_not_positive(frame):
    row = frame(_row(tpep_dropoff_datetime=datetime(2026, 1, 10, 8, 0))).first()
    assert row.avg_speed_mph is None


def test_flex_fare_flag(frame):
    assert frame(_row(payment_type=0, passenger_count=None)).first().is_flex_fare is True


def test_valid_trip_triggers_no_check(frame):
    df = frame(_row())
    flags = df.select([c.alias(n) for n, c in quality.logic_checks().items()]).first().asDict()
    assert not any(flags.values()), {k: v for k, v in flags.items() if v}


@pytest.mark.parametrize(
    ("name", "changes"),
    [
        ("passenger_count_zero", {"passenger_count": 0}),
        ("passenger_count_over_max", {"passenger_count": 7}),
        ("distance_zero_or_negative", {"trip_distance": 0.0}),
        ("distance_over_max", {"trip_distance": 150.0, "tpep_dropoff_datetime": datetime(2026, 1, 10, 11, 0)}),
        ("fare_zero_or_negative", {"fare_amount": -5.0}),
        ("total_zero_or_negative", {"total_amount": 0.0}),
        ("dropoff_before_pickup", {"tpep_dropoff_datetime": datetime(2026, 1, 10, 7, 0)}),
        ("duration_under_min", {"tpep_dropoff_datetime": datetime(2026, 1, 10, 8, 0, 30)}),
        ("duration_over_max", {"tpep_dropoff_datetime": datetime(2026, 1, 10, 12, 0)}),
        ("speed_over_max", {"trip_distance": 30.0}),
        ("pickup_outside_period", {"tpep_pickup_datetime": datetime(2025, 12, 31, 23, 50)}),
        ("ratecode_unknown", {"RatecodeID": 99}),
        ("location_unknown", {"PULocationID": 264}),
        ("cbd_fee_negative", {"cbd_congestion_fee": -0.75}),
    ],
)
def test_each_check_catches_its_case(frame, name, changes):
    assert _flags(frame(_row(**changes)), name) == [True]


def test_null_passenger_count_is_not_flagged_as_invalid(frame):
    df = frame(_row(payment_type=0, passenger_count=None, RatecodeID=None))
    assert _flags(df, "passenger_count_zero") == [False]
    assert _flags(df, "passenger_count_over_max") == [False]
    assert _flags(df, "ratecode_unknown") == [False]
