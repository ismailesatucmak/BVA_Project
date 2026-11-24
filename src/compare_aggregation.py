"""Compare Student 3 summaries on the same projected trip data."""

from contextlib import contextmanager, redirect_stdout
from io import StringIO
from pathlib import Path
from time import perf_counter

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import CLEAN_DIR, CLEAN_ROWS, RESULTS_DIR, ZONE_LOOKUP_PATH


TRIP_COLUMNS = [
    "pickup_hour", "tpep_pickup_datetime", "pickup_dow", "PULocationID",
    "payment_type", "fare_amount", "tip_amount", "total_amount",
]
PAYMENT_LABELS = {
    0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge",
    4: "Dispute", 5: "Unknown", 6: "Voided trip",
}
BOROUGHS = ("Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island")
SUMMARY_NAMES = {
    "hour": "Saat", "day": "Gün ve pencere", "pattern": "Hafta günü × saat",
    "zone": "Biniş bölgesi", "zone_concentration": "İlçede %80 eşiği", "payment": "Ödeme",
}
FLOAT_COLUMNS = {
    "trailing_7d_avg", "week_change_pct", "cumulative_pct", "observed_zones_pct",
    "mean_total_amount", "share_pct", "weighted_tip_pct",
}


def pandas_hourly(trips):
    return (trips.groupby("pickup_hour", dropna=False).size().reset_index(name="trip_count")
            .sort_values(["trip_count", "pickup_hour"], ascending=[False, True]))


def pandas_daily(trips):
    pickup_date = trips["tpep_pickup_datetime"].dt.normalize().rename("pickup_date")
    daily = trips.groupby(pickup_date, dropna=False).size().reset_index(name="trip_count")
    daily = daily.sort_values("pickup_date")
    counts = daily["trip_count"]
    previous = counts.shift(7)
    return daily.assign(
        previous_week_trips=previous,
        trailing_7d_avg=counts.rolling(7, min_periods=7).mean(),
        week_change_pct=100 * (counts - previous) / previous.where(previous.gt(0)),
    )


def pandas_weekday_hour(trips):
    return (trips.groupby(["pickup_dow", "pickup_hour"], dropna=False, sort=True)
            .size().reset_index(name="trip_count"))


def pandas_zones(trips, lookup):
    names = lookup.rename(columns={
        "LocationID": "PULocationID", "Borough": "borough", "Zone": "pickup_zone",
    })
    counts = trips.groupby("PULocationID", dropna=False).size().reset_index(name="trip_count")
    return (counts.merge(names, on="PULocationID", how="left", validate="many_to_one")
            .sort_values(["trip_count", "PULocationID"], ascending=[False, True]))


def pandas_concentration(zones):
    scoped = zones.loc[
        zones["borough"].isin(BOROUGHS) & ~zones["PULocationID"].isin([264, 265])
    ].sort_values(["borough", "trip_count", "PULocationID"], ascending=[True, False, True])
    by_borough = scoped.groupby("borough")
    ranked = scoped.assign(
        borough_total=by_borough["trip_count"].transform("sum"),
        observed_zones=by_borough["trip_count"].transform("size"),
        zone_rank=by_borough.cumcount() + 1,
        cumulative_trips=by_borough["trip_count"].cumsum(),
    )
    crossed = ranked.loc[5 * ranked["cumulative_trips"] >= 4 * ranked["borough_total"]]
    first = crossed.drop_duplicates("borough")
    return first.assign(
        cumulative_pct=100 * first["cumulative_trips"] / first["borough_total"],
        zones_to_80pct=first["zone_rank"],
        observed_zones_pct=100 * first["zone_rank"] / first["observed_zones"],
    ).sort_values("observed_zones_pct")


def pandas_payments(trips):
    eligible = (trips["payment_type"].eq(1) & trips["fare_amount"].gt(0)
                & trips["tip_amount"].ge(0))
    card = trips.loc[eligible, ["payment_type", "tip_amount", "fare_amount"]]
    card_stats = card.groupby("payment_type").agg(
        tip_eligible_trips=("tip_amount", "size"),
        eligible_tip_total=("tip_amount", "sum"),
        eligible_fare_total=("fare_amount", "sum"),
    )
    counts = trips.groupby("payment_type", dropna=False).agg(
        trip_count=("total_amount", "size"),
        mean_total_amount=("total_amount", "mean"),
    ).join(card_stats)
    fare = counts["eligible_fare_total"]
    return (counts.assign(
        tip_eligible_trips=counts["tip_eligible_trips"].fillna(0).astype("int64"),
        payment_label=counts.index.map(PAYMENT_LABELS),
        share_pct=100 * counts["trip_count"] / counts["trip_count"].sum(),
        weighted_tip_pct=100 * counts["eligible_tip_total"] / fare.where(fare.gt(0)),
    ).drop(columns=["eligible_tip_total", "eligible_fare_total"])
      .reset_index().sort_values(["trip_count", "payment_type"], ascending=[False, True]))


def pandas_reports(files, lookup_path, phases=None):
    started = perf_counter()
    trips = pq.read_table(list(files), columns=TRIP_COLUMNS).to_pandas()
    lookup = pd.read_csv(lookup_path, usecols=["LocationID", "Borough", "Zone"],
                         dtype={"LocationID": "int64"})
    prepared = perf_counter()
    zones = pandas_zones(trips, lookup)
    reports = {
        "hour": pandas_hourly(trips),
        "day": pandas_daily(trips),
        "pattern": pandas_weekday_hour(trips),
        "zone": zones,
        "zone_concentration": pandas_concentration(zones),
        "payment": pandas_payments(trips),
    }
    finished = perf_counter()
    if phases is not None:
        phases.update(preparation=prepared - started, summaries=finished - prepared)
    return reports, finished - started


def spark_reports(spark, files, lookup_path, phases=None):
    from src import aggregate_trips as queries

    started = perf_counter()
    trips = spark.read.parquet(*files).select(*TRIP_COLUMNS).persist()
    lookup = None
    zones = None
    try:
        lookup = spark.read.option("header", True).csv(str(lookup_path))
        if phases is not None:
            lookup = lookup.persist()
            # Force the lazy read and populate the cache before timing the six queries.
            input_rows = trips.count()
            if input_rows != CLEAN_ROWS or lookup.count() != 265:
                raise ValueError("The staged run received unexpected input counts.")
            prepared = perf_counter()
        zones = queries.zone_trip_counts(trips, lookup).persist()
        plans = {
            "hour": queries.hourly_trip_counts(trips),
            "day": queries.daily_window_metrics(queries.daily_trip_counts(trips)),
            "pattern": queries.weekday_hour_trip_counts(trips),
            "zone": zones,
            "zone_concentration": queries.zone_concentration(zones),
            "payment": queries.payment_trip_counts(trips),
        }
        reports = {
            name: pd.DataFrame.from_records(query.collect(), columns=query.columns)
            for name, query in plans.items()
        }
        finished = perf_counter()
        if phases is not None:
            phases.update(preparation=prepared - started, summaries=finished - prepared)
        return reports, finished - started
    finally:
        if zones is not None:
            zones.unpersist(blocking=True)
        if phases is not None and lookup is not None:
            lookup.unpersist(blocking=True)
        trips.unpersist(blocking=True)


def verify_reports(reference, candidate):
    """Require row order, missing-value patterns, and exact integer equality."""
    for name, expected in reference.items():
        observed = candidate[name]
        assert set(expected.columns) == set(observed.columns), name
        expected = expected.reset_index(drop=True)
        observed = observed[expected.columns].reset_index(drop=True)
        for column in expected:
            a, b = expected[column], observed[column]
            if column == "pickup_date":
                a = pd.to_datetime(a).astype("datetime64[ns]")
                b = pd.to_datetime(b).astype("datetime64[ns]")
            elif column in FLOAT_COLUMNS:
                pd.testing.assert_series_equal(a.astype(float), b.astype(float),
                                               check_exact=False, atol=1e-8, rtol=1e-12,
                                               obj=f"{name}.{column}")
                continue
            elif column in {"borough", "pickup_zone", "payment_label"}:
                a, b = a.astype("string"), b.astype("string")
            else:
                a, b = a.astype("Int64"), b.astype("Int64")
            pd.testing.assert_series_equal(a, b, check_exact=True, obj=f"{name}.{column}")


def verify_full_scope(reports):
    assert {name: len(table) for name, table in reports.items()} == {
        "hour": 24, "day": 151, "pattern": 168, "zone": 260,
        "zone_concentration": 5, "payment": 5,
    }
    for name in ("hour", "day", "pattern", "zone", "payment"):
        assert reports[name]["trip_count"].sum() == CLEAN_ROWS, name
    daily = reports["day"]
    dates = pd.DatetimeIndex(pd.to_datetime(daily["pickup_date"])).as_unit("ns")
    assert dates.equals(pd.date_range("2026-01-01", "2026-05-31"))
    assert daily["trailing_7d_avg"].isna().tolist() == [True] * 6 + [False] * 145
    assert daily["previous_week_trips"].isna().tolist() == [True] * 7 + [False] * 144
    assert abs(reports["payment"]["share_pct"].sum() - 100.0) < 1e-8


def clean_parquet_files():
    files = tuple(str(path) for path in sorted(Path(CLEAN_DIR).rglob("*.parquet")))
    assert len(files) == 5, files
    return files


@contextmanager
def arrow_workers():
    prior_threads = pa.cpu_count(), pa.io_thread_count()
    try:
        pa.set_cpu_count(2)
        pa.set_io_thread_count(2)
        yield
    finally:
        pa.set_cpu_count(prior_threads[0])
        pa.set_io_thread_count(prior_threads[1])


def benchmark(spark, repeats=4):
    """Run independent warm-up and measured reads without reusing runs' input caches."""
    files = clean_parquet_files()
    methods = {
        "PySpark": lambda: spark_reports(spark, files, ZONE_LOOKUP_PATH),
        "pandas + PyArrow": lambda: pandas_reports(files, ZONE_LOOKUP_PATH),
    }
    records = []
    with arrow_workers():
        reference, _ = methods["PySpark"]()
        classical, _ = methods["pandas + PyArrow"]()
        verify_full_scope(reference)
        verify_reports(reference, classical)
        stored = {name: pd.read_csv(RESULTS_DIR / f"s3_{name}.csv") for name in reference}
        verify_reports(reference, stored)
        order = tuple(methods)
        for n in range(1, repeats + 1):
            names = order if n % 2 else tuple(reversed(order))
            record = {"Tur": n, "İlk çalışan": names[0]}
            for name in names:
                results, seconds = methods[name]()
                verify_reports(reference, results)
                record[name] = seconds
            records.append(record)
        return reference, classical, pd.DataFrame.from_records(records).set_index("Tur")


def profile_prepared_pipeline(spark, reference, repeats=3):
    """Measure two stages with an explicit input-materialization boundary."""
    files = clean_parquet_files()
    methods = {
        "PySpark": lambda phases: spark_reports(spark, files, ZONE_LOOKUP_PATH, phases),
        "pandas + PyArrow": lambda phases: pandas_reports(files, ZONE_LOOKUP_PATH, phases),
    }
    phases_report = []
    order = tuple(methods)
    with arrow_workers():
        for name in order:
            reports, _ = methods[name]({})
            verify_reports(reference, reports)
        for turn in range(1, repeats + 1):
            names = order if turn % 2 else tuple(reversed(order))
            for name in names:
                phases = {}
                reports, _ = methods[name](phases)
                verify_reports(reference, reports)
                phases_report.append({"Tur": turn, "Yöntem": name, **phases})

    return pd.DataFrame.from_records(phases_report), hourly_spark_plan(spark)


def hourly_spark_plan(spark):
    """Explain the hour query with the projected, reusable input used by spark_reports."""
    from src.aggregate_trips import hourly_trip_counts

    # Planning happens after the timings and does not launch a separate scan.
    plan = StringIO()
    cached_input = spark.read.parquet(*clean_parquet_files()).select(*TRIP_COLUMNS).persist()
    try:
        with redirect_stdout(plan):
            hourly_trip_counts(cached_input).explain()
    finally:
        cached_input.unpersist(blocking=True)
    markers = ("FileScan parquet", "InMemoryTableScan", "HashAggregate",
               "hashpartitioning", "Sort [")
    return [line.strip() for line in plan.getvalue().splitlines()
            if any(marker in line for marker in markers)]


def saturday_context(spark):
    """Verify the peak-day breakdown by local hour and selected TLC zone groups."""
    from pyspark.sql import functions as F

    columns = ["pickup_dow", "pickup_hour", "PULocationID", "tpep_pickup_datetime"]
    keys = ["pickup_date", "pickup_hour", "area"]
    lookup = pd.read_csv(ZONE_LOOKUP_PATH)
    districts = {
        "Broadway çevresi (6 bölge)": [90, 100, 164, 186, 230, 234],
        "Upper East Side North": [236],
        "Upper East Side South": [237],
        "JFK ve LaGuardia": [132, 138],
    }
    assigned = {code for ids in districts.values() for code in ids}
    districts["Diğer Manhattan"] = sorted(set(lookup.loc[
        lookup["Borough"].eq("Manhattan"), "LocationID"]) - assigned)
    area_labels = {code: group for group, ids in districts.items() for code in ids}

    trips = spark.read.parquet(str(CLEAN_DIR)).select(*columns)
    area = F.create_map(*(value for code, group in area_labels.items()
                          for value in (F.lit(int(code)), F.lit(group))))
    grouped = (trips.where(F.col("pickup_dow") == 7)
               .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
               .withColumn("area", F.coalesce(area[F.col("PULocationID")], F.lit("Diğer bölgeler")))
               .groupBy(*keys).count().withColumnRenamed("count", "trip_count").orderBy(*keys))
    expected = pd.DataFrame.from_records(grouped.collect(), columns=grouped.columns)
    expected["pickup_date"] = pd.to_datetime(expected["pickup_date"]).astype("datetime64[ns]")

    files = sorted(Path(CLEAN_DIR).rglob("*.parquet"))
    local = pq.read_table(files, columns=columns).to_pandas()
    saturday = local.loc[local["pickup_dow"].eq(7), columns[1:]].assign(
        pickup_date=lambda rows: rows["tpep_pickup_datetime"].dt.normalize().astype("datetime64[ns]"),
        area=lambda rows: rows["PULocationID"].map(area_labels).fillna("Diğer bölgeler"),
    )
    actual = saturday.groupby(keys).size().reset_index(name="trip_count")
    pd.testing.assert_frame_equal(expected, actual, check_dtype=False, check_exact=True)
    assert expected["pickup_date"].nunique() == 22
    return expected
