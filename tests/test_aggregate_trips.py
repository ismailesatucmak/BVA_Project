from src.aggregate_trips import daily_trip_counts, hourly_trip_counts, payment_trip_counts, zone_trip_counts


def test_hourly_trip_counts_counts_rows_and_orders_ties(spark):
    trips = spark.sql("SELECT * FROM VALUES (18), (8), (18), (12) AS trips(pickup_hour)")

    counts = hourly_trip_counts(trips).collect()

    assert [(row.pickup_hour, row.trip_count) for row in counts] == [
        (18, 2),
        (8, 1),
        (12, 1),
    ]

def test_daily_trip_counts_groups_times_by_calendar_date(spark):
    trips = spark.sql("""
        SELECT * FROM VALUES
            (TIMESTAMP_NTZ '2026-02-01 00:00:00'),
            (TIMESTAMP_NTZ '2026-01-31 23:59:59'),
            (TIMESTAMP_NTZ '2026-02-01 18:00:00')
        AS trips(tpep_pickup_datetime)
    """)

    counts = daily_trip_counts(trips).collect()

    assert [(row.pickup_date.isoformat(), row.trip_count) for row in counts] == [
        ("2026-01-31", 1),
        ("2026-02-01", 2),
    ]

def test_zone_counts_keep_unmatched_ids(spark):
    trips = spark.sql("SELECT * FROM VALUES (10), (10), (20) AS trips(PULocationID)")
    zones = spark.sql("SELECT 10 AS LocationID, 'Manhattan' AS Borough, 'Test Zone' AS Zone")

    rows = zone_trip_counts(trips, zones).collect()

    assert [(row.PULocationID, row.pickup_zone, row.trip_count) for row in rows] == [
        (10, "Test Zone", 2),
        (20, None, 1),
    ]


def test_payment_counts_preserve_flex_fare_and_sum_to_100(spark):
    trips = spark.sql("SELECT payment_type, 20.0 AS total_amount, 10.0 AS fare_amount, 2.0 AS tip_amount FROM VALUES (0), (0), (1), (2) AS trips(payment_type)")

    rows = payment_trip_counts(trips).collect()

    assert [(row.payment_label, row.share_pct) for row in rows] == [
        ("Flex Fare", 50.0),
        ("Credit card", 25.0),
        ("Cash", 25.0),
    ]


def test_payment_tip_percentage_uses_eligible_totals(spark):
    trips = spark.sql("""
        SELECT * FROM VALUES
            (1, 12.0, 10.0, 2.0), (1, 33.0, 30.0, 3.0),
            (1,  5.0,  0.0, 5.0), (1, 19.0, 20.0, -1.0),
            (2, 15.0, 10.0, 5.0)
        AS trips(payment_type, total_amount, fare_amount, tip_amount)
    """)

    rows = {row.payment_type: row for row in payment_trip_counts(trips).collect()}

    assert rows[1].trip_count == 4
    assert rows[1].tip_eligible_trips == 2
    assert rows[1].weighted_tip_pct == 12.5
    assert rows[2].tip_eligible_trips == 0
    assert rows[2].weighted_tip_pct is None


def test_weekday_hour_counts_group_both_dimensions(spark):
    from src.aggregate_trips import weekday_hour_trip_counts

    trips = spark.sql("""
        SELECT * FROM VALUES (2, 18), (2, 18), (2, 21), (7, 18)
        AS trips(pickup_dow, pickup_hour)
    """)
    rows = weekday_hour_trip_counts(trips).collect()

    assert [(row.pickup_dow, row.pickup_hour, row.trip_count) for row in rows] == [
        (2, 18, 2),
        (2, 21, 1),
        (7, 18, 1),
    ]


def test_daily_windows_compare_separate_dates(spark):
    from src.aggregate_trips import daily_window_metrics

    daily = spark.range(8).selectExpr(
        "date_add(DATE '2026-01-01', CAST(id AS INT)) AS pickup_date",
        "10 * (id + 1) AS trip_count",
    )
    rows = daily_window_metrics(daily).collect()

    assert rows[5].trailing_7d_avg is None
    assert rows[6].trailing_7d_avg == 40
    assert rows[6].previous_week_trips is None
    assert rows[7].previous_week_trips == 10
    assert rows[7].week_change_pct == 700


def test_zone_concentration_accepts_exactly_eighty_percent(spark):
    from src.aggregate_trips import zone_concentration

    zones = spark.sql("""
        SELECT * FROM VALUES (1, 'Queens', 60), (2, 'Queens', 20),
                              (3, 'Queens', 20), (264, 'Queens', 10)
        AS zones(PULocationID, borough, trip_count)
    """)
    row = zone_concentration(zones).first()

    assert row.PULocationID == 2
    assert row.observed_zones == 3
    assert row.zones_to_80pct == 2
    assert row.cumulative_pct == 80
