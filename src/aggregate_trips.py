"""Pickup counts for the NYC Taxi aggregation analysis."""

import argparse
import csv
from datetime import date, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.config import CLEAN_DIR, CLEAN_ROWS, RESULTS_DIR, ZONE_LOOKUP_PATH
from src.spark_session import get_spark


def hourly_trip_counts(trips: DataFrame) -> DataFrame:
    """Count pickups by New York local hour, busiest first."""
    return (
        trips.groupBy("pickup_hour")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy(F.desc("trip_count"), F.asc("pickup_hour"))
    )


def daily_trip_counts(trips: DataFrame) -> DataFrame:
    """Count pickups per New York calendar date, earliest first."""
    return (
        trips.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .groupBy("pickup_date")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy("pickup_date")
    )


def daily_window_metrics(daily: DataFrame) -> DataFrame:
    """Compare calendar days and compute a full trailing seven-day average."""
    ordered = Window.orderBy("pickup_date")
    trailing = ordered.rowsBetween(-6, 0)
    return (
        daily.withColumn("previous_week_trips", F.lag("trip_count", 7).over(ordered))
        .withColumn("trailing_7d_avg", F.when(F.count("trip_count").over(trailing) == 7,
                                               F.avg("trip_count").over(trailing)))
        .withColumn("week_change_pct", F.when(F.col("previous_week_trips") > 0,
                    100.0 * (F.col("trip_count") - F.col("previous_week_trips")) / F.col("previous_week_trips")))
        .orderBy("pickup_date")
    )


def weekday_hour_trip_counts(trips: DataFrame) -> DataFrame:
    """Count pickups by Spark weekday (Sunday=1) and New York local hour."""
    return (
        trips.groupBy("pickup_dow", "pickup_hour")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy("pickup_dow", "pickup_hour")
    )


def zone_trip_counts(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Count pickups by zone ID and attach the TLC zone and borough names."""
    lookup = zones.select(
        F.col("LocationID").cast("int").alias("PULocationID"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    counts = trips.groupBy("PULocationID").count().withColumnRenamed("count", "trip_count")
    return counts.join(F.broadcast(lookup), "PULocationID", "left").orderBy(
        F.desc("trip_count"), F.asc("PULocationID")
    )


def zone_concentration(zones: DataFrame) -> DataFrame:
    """Rank recorded zones and find the first prefix containing at least 80%."""
    districts = ("Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island")
    borough = Window.partitionBy("borough")
    ranked = borough.orderBy(F.desc("trip_count"), F.asc("PULocationID"))
    cumulative = ranked.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        zones.filter(F.col("borough").isin(*districts) & ~F.col("PULocationID").isin(264, 265))
        .withColumn("borough_total", F.sum("trip_count").over(borough))
        .withColumn("observed_zones", F.count("*").over(borough))
        .withColumn("zone_rank", F.row_number().over(ranked))
        .withColumn("cumulative_trips", F.sum("trip_count").over(cumulative))
        .withColumn("cumulative_pct", 100.0 * F.col("cumulative_trips") / F.col("borough_total"))
        .withColumn("zones_to_80pct", F.min(F.when(
            5 * F.col("cumulative_trips") >= 4 * F.col("borough_total"), F.col("zone_rank")
        )).over(borough))
        .filter(F.col("zone_rank") == F.col("zones_to_80pct"))
        .withColumn("observed_zones_pct", 100.0 * F.col("zones_to_80pct") / F.col("observed_zones"))
        .orderBy("observed_zones_pct")
    )


def payment_trip_counts(trips: DataFrame) -> DataFrame:
    """Count payment codes and calculate their share of all pickups."""
    labels = F.create_map(
        F.lit(0), F.lit("Flex Fare"),
        F.lit(1), F.lit("Credit card"),
        F.lit(2), F.lit("Cash"),
        F.lit(3), F.lit("No charge"),
        F.lit(4), F.lit("Dispute"),
        F.lit(5), F.lit("Unknown"),
        F.lit(6), F.lit("Voided trip"),
    )
    eligible = (F.col("payment_type") == 1) & (F.col("fare_amount") > 0) & (F.col("tip_amount") >= 0)
    counts = trips.groupBy("payment_type").agg(
        F.count("*").alias("trip_count"),
        F.avg("total_amount").alias("mean_total_amount"),
        F.count(F.when(eligible, 1)).alias("tip_eligible_trips"),
        F.sum(F.when(eligible, F.col("tip_amount"))).alias("eligible_tip_total"),
        F.sum(F.when(eligible, F.col("fare_amount"))).alias("eligible_fare_total"),
    )
    return (
        counts.withColumn("payment_label", labels[F.col("payment_type").cast("int")])
        .withColumn("share_pct", 100.0 * F.col("trip_count") / F.sum("trip_count").over(Window.partitionBy()))
        .withColumn("weighted_tip_pct", F.when(F.col("eligible_fare_total") > 0,
                    100.0 * F.col("eligible_tip_total") / F.col("eligible_fare_total")))
        .drop("eligible_tip_total", "eligible_fare_total")
        .orderBy(F.desc("trip_count"), F.asc("payment_type"))
    )


def print_summary(analysis, key, rows, columns, total, *, limit):
    titles = {
        "hour": ("PICKUP HOUR (BİNİŞ SAATİ)", "Peak hour (En yoğun saat)"),
        "day": ("PICKUP DATE (BİNİŞ TARİHİ)", "Peak day (En yoğun gün)"),
        "zone": ("PICKUP ZONE (BİNİŞ BÖLGESİ)", "Peak zone (En yoğun bölge)"),
        "payment": ("PAYMENT TYPE (ÖDEME TÜRÜ)", "Top payment (En yaygın ödeme)"),
        "pattern": ("WEEKDAY × HOUR (HAFTA GÜNÜ × SAAT)", "Peak slot (En yoğun gün ve saat)"),
    }
    headings = {
        "pickup_hour": "Hour (Saat, New York)", "pickup_date": "Date (Tarih)",
        "PULocationID": "Zone ID (Bölge kodu)", "borough": "Borough (İlçe)",
        "pickup_zone": "Pickup zone (Biniş bölgesi)",
        "payment_type": "Payment code (Ödeme kodu)", "payment_label": "Payment type (Ödeme türü)",
        "share_pct": "Share (Pay)", "trip_count": "Trips (Yolculuk sayısı)",
        "pickup_dow": "Weekday (Hafta günü)",
        "previous_week_trips": "Previous week (Geçen haftanın aynı günü)",
        "trailing_7d_avg": "7-day average (7 günlük ortalama)",
        "week_change_pct": "Weekly change (Haftalık değişim)",
        "mean_total_amount": "Mean recorded total (Ortalama kayıtlı tutar)",
        "tip_eligible_trips": "Eligible trips (Bahşiş hesabındaki yolculuk)",
        "weighted_tip_pct": "Weighted tip (Ağırlıklı bahşiş)",
    }
    payments = {
        "Credit card": "Kredi kartı", "Cash": "Nakit", "No charge": "Ücretsiz",
        "Dispute": "İtirazlı", "Unknown": "Bilinmiyor", "Voided trip": "İptal",
        "Flex Fare": "Esnek ücret kategorisi",
    }
    zone_notes = {
        237: "Manhattan'da Central Park'ın doğusundaki mahallenin güney kesimi",
        236: "Upper East Side'ın kuzey kesimi",
        161: "Midtown'ın merkez kesimi",
        132: "John F. Kennedy Havalimanı",
        162: "Midtown'ın doğu kesimi",
        186: "Penn İstasyonu / Madison Meydanı'nın batısı",
        142: "Lincoln Meydanı'nın doğusu",
        230: "Times Meydanı / Tiyatro Bölgesi",
        79: "East Village mahallesi",
        239: "Upper West Side'ın güney kesimi",
    }
    weekdays = (
        "Sunday (Pazar)", "Monday (Pazartesi)", "Tuesday (Salı)", "Wednesday (Çarşamba)",
        "Thursday (Perşembe)", "Friday (Cuma)", "Saturday (Cumartesi)",
    )
    limits = {
        "hour": "Yoğunluğun nedenini veya her gün aynı saatin zirvede olduğunu göstermez.",
        "day": "Zirvenin bir etkinlikten, tatilden veya hava durumundan kaynaklandığını kanıtlamaz.",
        "zone": "Kişi başı taksi kullanımını, varış bölgelerini veya karşılanamayan talebi göstermez.",
        "payment": "Yolcuların ödeme tercihinin nedenini veya ödeme türlerinin gelir payını göstermez.",
        "pattern": "Ham hücre sayıları, hafta günleri farklı sıklıkta görüldüğü için günlük ortalama yerine kullanılamaz.",
    }

    def number(value):
        return f"{value:,}".replace(",", ".")

    def display(column, row):
        value = row[column]
        if value is None:
            return "—"
        if column == "mean_total_amount":
            return f"{value:.2f}".replace(".", ",") + " USD"
        if column == "trailing_7d_avg":
            return number(round(value))
        if column == "pickup_hour":
            return f"{value:02d}.00–{value:02d}.59"
        if column == "pickup_date":
            return value.strftime("%d.%m.%Y")
        if column == "payment_label":
            return f"{value} ({payments[value]})"
        if column == "pickup_zone":
            note = zone_notes.get(row.PULocationID, f"{row.borough} biniş bölgesi")
            return f"{value} ({note})"
        if column in ("share_pct", "week_change_pct", "weighted_tip_pct"):
            return f"%{value:.2f}".replace(".", ",")
        if column in ("trip_count", "previous_week_trips", "tip_eligible_trips"):
            return number(value)
        if column == "pickup_dow":
            return weekdays[value - 1]
        return str(value)

    title, peak_label = titles[analysis]
    print(f"\n{title}")
    print(" | ".join(headings[column] for column in columns))
    for row in rows[:10] if limit else rows:
        print(" | ".join(display(column, row) for column in columns))
    busiest = max(rows, key=lambda row: row.trip_count)
    peak = display(key, busiest)
    if analysis == "pattern":
        peak += f" {display('pickup_hour', busiest)}"
    print(f"{peak_label}: {peak} ({number(busiest.trip_count)} yolculuk)")
    print(f"Total (Toplam): {number(total)} yolculuk ({len(rows)} grup)")
    share = f"{100 * busiest.trip_count / total:.2f}".replace(".", ",")
    if analysis == "payment":
        finding = f"{peak}, temiz verideki yolculukların %{share}'ini oluşturuyor."
    else:
        finding = f"{peak}, temiz veride en fazla biniş kaydedilen grup (toplamın %{share}'i)."
    print(f"Finding (Çıkarım): {finding}")
    print(f"Limitation (Sınır): {limits[analysis]}")


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=tuple(rows[0].asDict()))
        writer.writeheader()
        writer.writerows(row.asDict() for row in rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="NYC Taxi biniş ve ödeme özetleri")
    analyses = ("hour", "day", "zone", "payment")
    parser.add_argument("--analysis", choices=(*analyses, "pattern", "all"), default="hour")
    parser.add_argument("--export", action="store_true", help="Doğrulanmış özetleri reports/results klasörüne yaz")
    args = parser.parse_args()

    spark = get_spark("nyc-taxi-aggregation", driver_memory="1g", cores="2")
    try:
        spark.sparkContext.setLogLevel("ERROR")
        trips = spark.read.parquet(str(CLEAN_DIR))
        queries = {
            "hour": (hourly_trip_counts, "pickup_hour"),
            "day": (lambda trips: daily_window_metrics(daily_trip_counts(trips)), "pickup_date"),
            "pattern": (weekday_hour_trip_counts, "pickup_dow"),
            "payment": (payment_trip_counts, "payment_label"),
        }
        selected = analyses if args.analysis == "all" else (args.analysis,)
        if args.analysis == "all" and args.export:
            selected = (*selected, "pattern")
        for analysis in selected:
            if analysis == "zone":
                lookup = spark.read.option("header", True).csv(str(ZONE_LOOKUP_PATH))
                key = "pickup_zone"
                zone_summary = zone_trip_counts(trips, lookup)
                rows = zone_summary.collect()
                concentration = zone_concentration(zone_summary).collect()
                if len(concentration) != 5:
                    raise ValueError("Expected five NYC borough summaries")
            else:
                query, key = queries[analysis]
                rows = query(trips).collect()

            total = sum(row.trip_count for row in rows)
            if total != CLEAN_ROWS or any(row[key] is None for row in rows):
                raise ValueError(f"Invalid {analysis} summary: {total:,} trips or missing group labels")
            if analysis == "day":
                start, end = date(2026, 1, 1), date(2026, 5, 31)
                expected_dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
                if [row.pickup_date for row in rows] != expected_dates:
                    raise ValueError("Expected a complete calendar from January through May 2026")
            if analysis == "hour" and {row.pickup_hour for row in rows} != set(range(24)):
                raise ValueError("Expected one group for each hour from 0 through 23")
            if analysis == "pattern" and {(row.pickup_dow, row.pickup_hour) for row in rows} != {
                (day, hour) for day in range(1, 8) for hour in range(24)
            }:
                raise ValueError("Expected 168 weekday-hour groups")
            if analysis == "payment" and abs(sum(row.share_pct for row in rows) - 100.0) > 1e-8:
                raise ValueError("Expected payment shares to add up to 100%")

            columns = tuple(rows[0].asDict())
            if args.export:
                RESULTS_DIR.mkdir(parents=True, exist_ok=True)
                output = RESULTS_DIR / f"s3_{analysis}.csv"
                write_csv(output, rows)
                if analysis == "zone":
                    write_csv(RESULTS_DIR / "s3_zone_concentration.csv", concentration)
            if analysis == "pattern" and args.analysis == "all":
                continue
            print_summary(analysis, key, rows, columns, total, limit=args.analysis == "all")
            if analysis == "zone":
                print("Binişlerin en az %80’ine ulaşan bölge sayısı:")
                for row in concentration:
                    print(f"  {row.borough}: {row.zones_to_80pct} / {row.observed_zones} bölge")
                exceptions = {row.PULocationID: row.trip_count for row in rows}
                print(f"  Ayrı kontrol: Newark {exceptions.get(1, 0)}; "
                      f"264 (bilinmeyen) {exceptions.get(264, 0)}; "
                      f"265 (NYC dışı) {exceptions.get(265, 0)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
