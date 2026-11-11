"""Veri kalitesi tanımları — profil (T2.1) ve temizlik (T2.2) aynı tanımları kullanır.

* add_derived(): süre, hız, saat, gün ve Flex Fare kolonlarını ekler.
* logic_checks(): her tutarsızlık için True/False üreten Spark ifadeleri.
  Null giriş hiçbir zaman "hatalı" sayılmaz (coalesce → False): Flex Fare kayıtlarındaki
  null'lar (bkz. data_dictionary §4) eksik değil, iş kuralının sonucudur.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src import config

SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60


def _duration_minutes() -> Column:
    # Saat dilimsiz (NTZ) zaman damgalarında duvar saati farkı; yaz saati geçişinde
    # (8 Mart 2026, 02:00) bu saati kapsayan yolculuklar ±60 dk sapabilir (veri özelliği).
    seconds = F.expr("timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime)")
    return seconds / SECONDS_PER_MINUTE


def add_derived(df: DataFrame) -> DataFrame:
    duration = _duration_minutes()
    return df.withColumns(
        {
            "trip_duration_min": duration,
            "avg_speed_mph": F.when(
                duration > 0, F.col("trip_distance") / (duration / MINUTES_PER_HOUR)
            ),
            "pickup_hour": F.hour("tpep_pickup_datetime"),
            "pickup_dow": F.dayofweek("tpep_pickup_datetime"),
            "is_flex_fare": F.col("payment_type") == config.FLEX_FARE_PAYMENT_TYPE,
        }
    )


def _flag(condition: Column) -> Column:
    return F.coalesce(condition, F.lit(False))


def logic_checks() -> dict[str, Column]:
    """Ad → bool ifade. Sıra raporlardaki sırayı belirler. Girdi add_derived() çıktısı olmalı."""
    duration = F.col("trip_duration_min")
    pickup = F.col("tpep_pickup_datetime")
    checks = {
        "passenger_count_zero": F.col("passenger_count") == 0,
        "passenger_count_over_max": F.col("passenger_count") > config.MAX_PASSENGERS,
        "distance_zero_or_negative": F.col("trip_distance") <= 0,
        "distance_over_max": F.col("trip_distance") > config.MAX_TRIP_MILES,
        "fare_zero_or_negative": F.col("fare_amount") <= 0,
        "total_zero_or_negative": F.col("total_amount") <= 0,
        "dropoff_before_pickup": duration < 0,
        "duration_under_min": (duration >= 0) & (duration <= config.MIN_DURATION_MIN),
        "duration_over_max": duration > config.MAX_DURATION_MIN,
        "speed_over_max": F.col("avg_speed_mph") > config.MAX_SPEED_MPH,
        "pickup_outside_period": (pickup < F.lit(config.PERIOD_START).cast("timestamp_ntz"))
        | (pickup >= F.lit(config.PERIOD_END).cast("timestamp_ntz")),
        "ratecode_unknown": F.col("RatecodeID") == config.UNKNOWN_RATECODE,
        "location_unknown": F.col("PULocationID").isin(*config.UNKNOWN_LOCATION_IDS)
        | F.col("DOLocationID").isin(*config.UNKNOWN_LOCATION_IDS),
        "cbd_fee_negative": F.col("cbd_congestion_fee") < 0,
    }
    return {name: _flag(cond) for name, cond in checks.items()}
