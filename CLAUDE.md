# CLAUDE.md — NYC Taxi Büyük Veri Analitiği (BVA_Project)

Bu dosya, depoyu açan AI asistanına (Claude Code vb.) projeyi tanıtır. İnsan okurlar için ana belge `README.md`'dir.

## Proje
- 5 öğrencilik ders projesi. Araştırma sorusu: *veri büyüdükçe klasik yaklaşım (pandas + CSV) ne zaman yetersiz kalıyor?*
- Veri: NYC TLC Yellow Taxi 2026 Ocak–Mayıs, 18.999.282 ham kayıt → 17.210.961 temiz kayıt.
- **Öğrenci 1** (temin, sözlük, format, boyut analizi) ve **Öğrenci 2** (temizlik, anomali) **tamamlandı** — kodlarını değiştirmeden önce sahibine sorun.
- Öğrenci 3 (aggregation), 4 (ML), 5 (pandas vs Spark **işleme** performansı + dashboard) bu depoya kendi bölümlerini ekler.
- Resmi gereksinimler ve puanlama: `docs/PROJECT_REQUIREMENTS.md`. Kararlar ve durum: `docs/ROADMAP.md`.

## Kurulum (Windows)
1. Proje yolunda **Türkçe karakter olmamalı** (Spark/Hadoop bozulur). Boşluk sorun değil.
2. Temurin **JDK 17** → varsayılan `C:\tools\jdk-17` (farklıysa `NYC_JAVA_HOME`). Java 21 ile PySpark 3.5 kullanmayın.
3. `winutils.exe` + `hadoop.dll` (Hadoop 3.3.6) → `C:\tools\hadoop\bin` (farklıysa `HADOOP_HOME`).
4. `python -m venv .venv` → `.\.venv\Scripts\activate` → `pip install -r requirements.txt` (Python 3.11).
5. `python scripts/smoke_test_spark.py` sonunda "TÜM TESTLER BAŞARILI" yazmalı.

## Veri (Git'te YOK — `data/` gitignore'da, dosyalar 100 MB sınırını aşıyor)
| İhtiyaç | Komut |
|---|---|
| Temiz veri `data/clean` (S3/S4/S5 girdisi) | `python -m src.fetch_data` — Release `data-v1` ekini indirir, SHA-256 ve satır sayısını doğrular |
| Ham veri `data/raw` | `python -m src.ingest` |
| Temiz veriyi yeniden üretmek | `python -m src.ingest` → `python -m src.clean` |
| Boyut setleri (5,2 GB) | `python -m src.build_sizes` → `NYC_DATA_ROOT` / `D:\BVAVeri` / `data/large` |

Temiz veri şeması ve garantileri: `docs/data_dictionary.md` §8. Okuma: `get_spark().read.parquet("data/clean")`.

## Kurallar
- Spark'ı **her zaman** `src.spark_session.get_spark()` ile açın (Java/Hadoop yolları, localhost düzeltmesi, `spark.local.dir`). Kendi `SparkSession.builder`'ınızı yazmayın.
- Yollar ve eşikler `src/config.py`'de; sabit değer gömmeyin.
- Veri kalitesi tanımları tek yerde: `src/quality.py` (`add_derived`, `logic_checks`). Yeniden tanımlamayın.
- Veri dosyalarını, zip'leri, `data/` içeriğini **commit etmeyin**.
- Commit mesajı: `<type>: <açıklama>` (feat, fix, docs, test, refactor, chore).
- Testler: `python -m pytest tests` — yeni kod için test ekleyin.
- Yeni dosya adları: notebook `notebooks/03_…`/`04_…`/`05_…`, grafik `reports/figures/11_…` ve sonrası, sonuç `reports/results/…csv`.
- Kapsam sınırı: Öğrenci 1 **okuma/depolama** performansını ölçtü; groupby/join gibi **işleme** performansı Öğrenci 5'indir. Aynı ölçümü tekrarlamayın, `reports/results/io_benchmark.csv`'yi referans verin.

## Veriyle ilgili bilinmesi gerekenler
- **Flex Fare:** `payment_type = 0` olan kayıtlarda (`is_flex_fare = True`, temiz verinin %25,5'i) `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee` **null**'dur. Bu hata değil; `dropna()` kullanmayın, bu grubu ayrı ele alın.
- **Helix (VendorID 7) temiz veride yok** (iniş zamanı gönderilmiyor → tamamı süre kuralıyla silindi).
- `RatecodeID = 99` kayıtları silindi → dış ilçelerin (Bronx, Brooklyn) temiz verideki payı azaldı. İlçe analizinde belirtin.
- `fare_amount` hâlâ güzergâh bazlı uç değerler içerir (silinmedi, `notebooks/02` §5'te analiz edildi).
- Zaman damgaları saat dilimsiz (`timestamp_ntz`, New York yerel saati). `pickup_dow`: 1 = Pazar … 7 = Cumartesi.

## Bilinen tuzaklar (bu projede yaşandı)
| Belirti | Neden / Çözüm |
|---|---|
| `Invalid Spark URL` | bilgisayar adında `_` → `get_spark()` localhost kullanır |
| `PATH_NOT_FOUND …%20…` | `Path.as_uri()` boşluğu `%20` yapar, Hadoop çözmez → Spark'a `str(path)` verin |
| Python listesinden `createDataFrame` ~35 sn | çekirdek başına Python işçisi açılır → testlerde `get_spark(cores="1")` (bkz. `tests/conftest.py`) |
| Parquet okuma süresi "anında" | `count()` metadata'dan cevap verir → `df.write.format("noop").mode("overwrite").save()` ile ölçün |
| `percentile_approx` ile JVM çöküşü | çok kolon × yüksek `accuracy` → 10.000 (varsayılan) kullanın |
| Çıktıda yüzlerce küçük dosya | window/shuffle sonrası 200 partition → yazmadan önce `repartition(<partition kolonları>)` |
| pandas 3 hataları | PySpark 3.5 pandas 2.x ister → `requirements.txt`'teki sürüm |
| Konsolda "Missing Python executable" uyarısı | yoldaki boşluk; zararsız |
