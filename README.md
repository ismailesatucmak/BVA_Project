# NYC Taxi Büyük Veri Analitiği

Milyonlarca taksi yolculuğundan şehir içi ulaşım davranışlarını analiz eden bir büyük veri projesi.

> **Araştırma sorusu:** Veri büyüdükçe klasik analiz yaklaşımı (tek makinede pandas + CSV) ne zaman yetersiz kalıyor, ve neden?

| Öğrenci | Görev | Durum |
|---|---|---|
| **Öğrenci 1** | Veriyi temin eder, veri sözlüğünü hazırlar, CSV/Parquet dönüşümünü ve veri büyüklüğü analizini yapar | ✅ |
| **Öğrenci 2** | Eksik/hatalı kayıtları temizler; anormal mesafe, ücret ve süre değerlerini analiz eder | ✅ |
| **Öğrenci 3** | Saat, gün, bölge ve ödeme türüne göre yolculukları analiz eder | ✅ |
| **Öğrenci 4** | PySpark MLlib ile yolculuk süresi tahmini (100K/1M/5M) ve scikit-learn ile adil kıyaslama | ✅ |
| **Öğrenci 5** | pandas vs Spark işleme performansı (groupby / join / window × CSV / Parquet × 5 boyut) + sonuç dashboard'u | ✅ |

---

## 0. Takım için hızlı başlangıç

**Depo:** https://github.com/ismailesatucmak/BVA_Project

```powershell
# 1) Klonla (proje yolunda Türkçe karakter OLMASIN — Spark/Hadoop Windows'ta bozuluyor)
git clone https://github.com/ismailesatucmak/BVA_Project.git
cd BVA_Project

# 2) Ortam (önce Java 17 + winutils kurulu olmalı → §3)
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python scripts/smoke_test_spark.py        # sonunda "TÜM TESTLER BAŞARILI" görmelisiniz

# 3) Temiz veriyi al (Öğrenci 3/4/5'in girdisi) — Release'den 479 MB indirir, doğrular, data/clean'e açar
python -m src.fetch_data
```

```python
# 4) Kullan
from src.spark_session import get_spark
df = get_spark().read.parquet("data/clean")    # 17.210.961 kayıt
```

**Veri nerede, nasıl alınır?** Veri dosyaları Git'te **yok** (`data/` `.gitignore`'da; tek dosyalar GitHub'ın 100 MB sınırını aşıyor).

| Veri | Boyut | Nasıl alınır |
|---|---|---|
| Temiz veri `data/clean` | 479 MB | `python -m src.fetch_data` → [Release `data-v1`](https://github.com/ismailesatucmak/BVA_Project/releases/tag/data-v1) eki, SHA-256 + satır sayısı doğrulamalı. Elle indirirseniz zip'i `data/clean/` içine açın (içinde `pickup_year=2026/` olmalı). |
| Ham veri `data/raw` | 310 MB | `python -m src.ingest` → resmi TLC kaynağından indirir ve doğrular |
| Boyut setleri (100K…19M × 4 format) | 5,2 GB | `python -m src.build_sizes` (~12 dk). Yalnızca format/boyut deneyi yapacaksanız gerekir (Öğrenci 5) |
| Ölçüm sonuçları, grafikler | küçük | Depoda: `reports/results/*.csv`, `reports/figures/*.png` |

Temiz veriyi kendiniz üretmek isterseniz: `python -m src.ingest` → `python -m src.clean` (~3 dk + indirme). Sonuç birebir aynıdır.

**Kendi bölümünüzü eklerken:** kodu `src/` altına, notebook'u `notebooks/03_…`, `04_…`, `05_…` olarak, sonuçları `reports/results/`, grafikleri `reports/figures/11_…` ve sonrası olarak koyun; README'de kendi bölümünüzü (§8–10) doldurun. Yeni eşikleri `src/config.py`'ye, Spark oturumunu her zaman `get_spark()` ile açın. Claude Code kullanıyorsanız [`CLAUDE.md`](CLAUDE.md) projeyi ona otomatik tanıtır.

---

## 1. Veri

- **Kaynak:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — Yellow Taxi, **2026 Ocak–Mayıs**
- **18.999.282 kayıt**, 20 kolon, 5 Parquet dosyası (310 MB, ZSTD) + taxi zone lookup (265 bölge)
- Gerçek veri yeterince büyük olduğu için sentetik çoğaltma yapılmadı.
- Kolon kolon açıklama, birim, kod tabloları ve gözlenen istatistikler: [`docs/data_dictionary.md`](docs/data_dictionary.md)

## 2. Mimari

![Mimari şema](docs/architecture.png)

TLC'den indirilen ham Parquet dosyaları iki koldan işlenir. **Öğrenci 1** veriyi 5 boyutta (100K → 19M) ve 4 formatta (CSV, Parquet snappy/gzip/zstd) yeniden yazar ve pandas ile Spark'ın okuma süresini ve bellek kullanımını ölçer. **Öğrenci 2** veriyi profiller ve kural bazlı temizler; çıkan temiz veri (`data/clean`) Öğrenci 3/4/5'in ortak girdisidir. Tüm ağır işler PySpark 3.5 ile yapılır. Şemayı yeniden üretmek için: `python scripts/make_architecture.py`.

## 3. Kurulum (Windows)

| Bileşen | Sürüm | Not |
|---|---|---|
| Python | 3.11 | |
| Java | Temurin JDK 17 | varsayılan `C:\tools\jdk-17`; farklıysa `NYC_JAVA_HOME` ortam değişkeni |
| Hadoop Windows araçları | `winutils.exe` + `hadoop.dll` 3.3.6 | `C:\tools\hadoop\bin`; farklıysa `HADOOP_HOME` — Spark'ın Windows'ta dosya yazabilmesi için gerekli |
| Python paketleri | `requirements.txt` | PySpark 3.5.9, pandas 2.3 (pandas 3 PySpark 3.5 ile uyumsuz), pyarrow |

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python scripts/smoke_test_spark.py      # Spark kurulum testi
```

- Büyük çıktılar (boyut setleri 5,2 GB, Spark geçici dosyaları) için kök klasör sırasıyla: `NYC_DATA_ROOT` ortam değişkeni → `D:\BVAVeri` (varsa; Öğrenci 1'in ölçümleri burada alındı) → proje içindeki `data/large`. Örn. `$env:NYC_DATA_ROOT = "E:\veri"`.
- Bilgisayar adı alt çizgi içeriyorsa (örn. `Ismail_Esat`) Spark URL'si geçersiz olur; `src/spark_session.py` bunu `localhost` ile çözer. Her yerde `get_spark()` kullanılır.

## 4. Çalıştırma sırası

| # | Komut | Ne yapar | Süre* |
|---|---|---|---|
| 1 | `python -m src.ingest` | eksik dosyaları TLC'den indirir (310 MB), var olanları doğrular | bağlantıya bağlı |
| 2 | `python -m src.dictionary_stats` | veri sözlüğü istatistikleri (Spark) | ~1 dk |
| 3 | `python -m src.build_sizes` | 5 boyut × 4 format = 20 dosya | ~12 dk |
| 4 | `python -m src.benchmark_io` · `--startup` | okuma süresi + bellek ölçümü (85 kombinasyon, 3 tekrar) | ~45 dk |
| 5 | `python -m src.profile_data` | ham veri profili + 14 tutarlılık kontrolü | ~1 dk |
| 6 | `python -m src.clean` | temizlik → `data/clean` | ~2 dk |
| 7 | `python -m src.anomaly` | anomali analizi (window + join) | ~3 dk |
| 8 | `notebooks/01_*.ipynb`, `notebooks/02_*.ipynb` | grafikler ve yorum | — |
| 9 | `python -m src.aggregate_trips --analysis all --export` | saati, günü, biniş bölgesini ve ödeme türünü özetler, CSV'leri yazar (§8) | — |
| 10 | `python -m src.ml_duration` · `python -m src.ml_analysis` | S4: örneklemler + 16 ML ölçümü + Şekil 18–20; sızıntı, arama, hata analizi + Şekil 24–25 (§9) | ~25 dk |
| 11 | `python -m src.benchmark_processing` · `python -m src.scale_synthetic` | S5: pandas vs Spark vs Spark + cache × CSV vs Parquet × 5 boyut × 3 işlem + sentetik 57M + dosya düzeni kontrolü (§10) | ~35 dk |
| 12 | `python -m src.plots_processing` · `python -m src.dashboard` | S5: Şekil 21–23, 26 ve `reports/dashboard/index.html` | < 1 dk |

\* 1–9: 28 çekirdek, 16 GB RAM, NVMe SSD (S1/S2 makinesi). 10–12: i7-13700H, 20 iş parçacığı, 32 GB RAM, performans modunda (dizüstüde dengeli modda süreler ~2,7× uzar). Her script kaldığı yerden devam eder veya sonucu yeniden üretir.
Yalnızca temiz veri gerekiyorsa bu tablonun yerine `python -m src.fetch_data` yeterlidir (§0).

**Testler:** `python -m pytest tests` (144 test; Spark testleri tek çekirdekle çalışır, ~1–2 dk).

## 5. Klasör yapısı

```
src/            ingest, dictionary_stats, build_sizes, benchmark_io, plots_io        (Öğrenci 1)
                quality, profile_data, clean, anomaly, plots_clean                    (Öğrenci 2)
                aggregate_trips, compare_aggregation                                  (Öğrenci 3)
                ml_duration, ml_analysis                                              (Öğrenci 4)
                benchmark_processing, scale_synthetic, plots_processing, dashboard    (Öğrenci 5)
                config (yollar + eşikler), spark_session (Windows ayarları),
                fetch_data (temiz veriyi Release'den kurar), mem_probe (tepe bellek)  (ortak)
notebooks/      01_data_acquisition_and_formats, 02_cleaning_and_anomalies,
                03_aggregation, 03_aggregation_ders, 04_ml_duration_prediction,
                04_ml_duration_ders, 05_processing_benchmark, 05_processing_ders
reports/        results/*.csv (ölçümler ve özetler) · figures/01–26_*.png · dashboard/index.html
docs/           ROADMAP, data_dictionary, cleaning_rules, technical_report_s1_s2/_s3/_s4/_s5,
                exam_questions_s1_s2/_s3/_s4/_s5, PROJECT_REQUIREMENTS, architecture.png
data/           raw/ (TLC) · clean/ (temiz veri) · samples/ — Git'e girmez
tests/          pytest
```

---

## 6. Öğrenci 1 — Veri temini, sözlük, format dönüşümü, veri büyüklüğü analizi

**Notebook:** [`notebooks/01_data_acquisition_and_formats.ipynb`](notebooks/01_data_acquisition_and_formats.ipynb)

| İş | Dosya | Teknik ayrıntı |
|---|---|---|
| Veri temini | `src/ingest.py` | eksik dosyayı indirir, yarım inen dosya bırakmaz (`.part` → yeniden adlandırma), şema ve kayıt sayısını doğrular |
| Veri sözlüğü | `src/dictionary_stats.py` → `docs/data_dictionary.md` | 19M kayıt üzerinde Spark ile tip, null, min/max, kod dağılımları; resmi PDF ile birleştirildi |
| Format dönüşümü | `src/build_sizes.py` | 100K / 1M / 5M / 10M / full; **xxhash64 tabanlı iç içe örneklem** (makineden bağımsız, 100K ⊂ 1M ⊂ … ⊂ full); her boyut × format tek dosya |
| Boyut analizi | `src/benchmark_io.py` | pandas vs Spark; tam ve 3 kolonluk okuma; her pandas ölçümü ayrı süreçte (tepe bellek); Spark `noop` yazımıyla tetiklenir (`count()` Parquet'te veriyi okumaz) |
| Grafikler | `src/plots_io.py` | `reports/figures/01–05` |

**Sonuçlar (19M kayıt):**

| | Disk | Okuma | Okurken en yüksek RAM |
|---|---|---|---|
| pandas + CSV | 1.907 MB | **71,1 sn** | **9,4 GB (%59)** |
| pandas + Parquet (zstd) | 290 MB | 2,0 sn | 6,6 GB |
| Spark + CSV (oturum açılışı hariç) | 1.907 MB | 8,1 sn | — |
| Spark + Parquet (snappy) | 392 MB | 3,7 sn | — |

- Parquet diskte CSV'den **4,9–6,6 kat küçük**; pandas ile **~40 kat hızlı** okunur.
- Spark oturum açılışı (11,4 sn) dahil edilince pandas + CSV, **~3,6M kayıttan** sonra Spark'ın gerisine düşer.
- pandas tepe belleği satır başına ~0,5 KB ile doğrusal artar → bu makinede **~32M kayıtta RAM tükenir**.
- **Cevap:** Bu ölçekte en büyük kazanç formattan gelir (CSV → Parquet). Klasik yaklaşımın sert sınırı ise bellektir; RAM'e sığmayan veride Spark zorunludur.

## 7. Öğrenci 2 — Temizlik ve anomali analizi

**Notebook:** [`notebooks/02_cleaning_and_anomalies.ipynb`](notebooks/02_cleaning_and_anomalies.ipynb) · **Kurallar:** [`docs/cleaning_rules.md`](docs/cleaning_rules.md)

| İş | Dosya | Teknik ayrıntı |
|---|---|---|
| Ortak kontroller | `src/quality.py` | türetilmiş kolonlar + 14 mantıksal kontrol; profil, temizlik ve anomali aynı tanımları kullanır |
| Profil | `src/profile_data.py` | 19M kayıtta yüzdelikler (p0,1–p99,9) ve kontrol sayıları, Flex Fare kırılımıyla |
| Temizlik | `src/clean.py` | 10 kural; her kural için tek başına / sıralı / yalnız bu kural etkisi; R9 duplicate **window** (`row_number`) |
| Anomali | `src/anomaly.py` | kural vs IQR / p99,9; güzergâh bağlamı **window** (`partitionBy(PU, DO)`); zone lookup **join** ile örüntü |
| Grafikler | `src/plots_clean.py` | `reports/figures/06–10` |

**Sonuçlar:**
- **Eksik veri sistematik:** null'ların tamamı `payment_type = 0` (Flex Fare) kayıtlarında (%25,3). Naif `dropna()` bu kayıtların hepsini silerdi; null tek başına silme nedeni sayılmadı, doldurma yapılmadı.
- **18.999.282 → 17.210.961 kayıt (%90,6 korundu).** Kurallar örtüşüyor: tek başına toplam 2,09M, sıralı toplam 1,79M.
- Temizlik ortalamaları değiştirir, medyanları değiştirmez: mesafe ortalaması 5,74 → 3,25 mil, medyan 1,83 → 1,84.
- **Kural ≠ istatistik:** R3 (ücret ≤ 0) ile p99,9 eşiği arasında örtüşme sıfır; global IQR ağır kuyruklu taksi ücretlerinde 1,25M meşru kaydı anomali sayıyor.
- **Bağlam:** güzergâh bazlı analiz, global eşiğin 15 katı ücret anomalisi buluyor (örn. aynı bölge içinde 0,15 mil için 650 $).
- **Hataların kaynağı sağlayıcılar:** Helix iniş zamanını göndermiyor (kayıtlarının tamamı silindi), Creative Mobile tarife kodunu göndermiyor, iadeler Curb'ün ters kayıtları.

**Temiz veri (Öğrenci 3/4/5 için):**
```python
from src.spark_session import get_spark
df = get_spark().read.parquet("data/clean")   # 17,2M kayıt; pickup_year/pickup_month partition
```
Şema ve garanti edilen koşullar: [`docs/data_dictionary.md` §8](docs/data_dictionary.md). Flex Fare kayıtlarında (`is_flex_fare = True`) 5 kolon null'dur.

## 8. Öğrenci 3 — Aggregation analizleri

**Notebook:** [`notebooks/03_aggregation_ders.ipynb`](notebooks/03_aggregation_ders.ipynb) · **Girdi:** Ocak–Mayıs 2026, 17.210.961 temiz Yellow Taxi yolculuğu

Ders notebook'u temiz Parquet verisinden altı özeti PySpark ile üretir ve sonuçların kapsamını kontrol eder. local[2] ile local[4] üzerinde sorgu sürelerini, ayrıca broadcast ve sort-merge birleştirme sürelerini gösterir.

| İş | Çıktı | Teknik ayrıntı |
|---|---|---|
| Saatlik hacim | [Saat grafiği](reports/figures/11_s3_hour.png), [ısı haritası](reports/figures/15_s3_weekday_hour.png) | `groupBy` ile 24 saat ve 168 hafta günü × saat grubu. Isı haritasında her hücre ilgili hafta gününün görülme sayısına bölünür. |
| Günlük hacim | [Gün grafiği](reports/figures/12_s3_day.png) | 151 takvim günü doğrulanır. Son 7 gün için hareketli ortalama, `lag(7)` ile önceki haftanın aynı gününe göre değişim. |
| Biniş bölgeleri | [İlk 10 bölge](reports/figures/13_s3_zone.png), [harita](reports/figures/16_s3_pickup_map.png), [ilçe karşılaştırması](reports/figures/17_s3_zone_concentration.png) | Bölge koduyla sayım → TLC tablosuyla `broadcast` + `left join`. İlçe içinde sıralı pencereyle en az %80'e ulaşılan ilk sıra. |
| Ödeme türleri | [Pay grafiği](reports/figures/14_s3_payment.png), [ödeme tablosu](reports/results/s3_payment.csv) | Yolculuk payı ve kayıt başına ortalama toplam tutar. Uygun kredi kartı kayıtlarında ağırlıklı bahşiş oranı: toplam kayıtlı bahşiş / toplam taksimetre ücreti. |

**Sonuçlar:**

- **Saat:** New York yerel saatiyle **18.00–18.59** en yoğun aralık (1.150.635 biniş). Hafta günü başına ortalama alındığında hafta içindeki en yoğun hücre perşembe 21.00 (8.830), hafta sonundaki cumartesi 18.00 (8.853).
- **Gün:** En yoğun gün **25 Nisan 2026** (153.232 biniş). Bir önceki cumartesiye (141.356) göre **%8,4 artış**.
- **Bölge:** Manhattan'da Central Park'ın doğusundaki **Upper East Side South** ilk sırada (815.297). İlçe binişlerinin en az %80'ine Queens'te **4/69**, Manhattan'da **30/67** gözlenen bölgeyle ulaşıldı. Newark (77 biniş) ayrı tutuldu. 264 ve 265 kodlarında temiz biniş kaydı yok.
- **Ödeme:** Kredi kartı **%64,96**, Flex Fare **%25,45**. Kredi kartı grubunda ortalama toplam tutar **29,05 USD**, ağırlıklı kayıtlı bahşiş oranı **%23,50** (11.180.868 uygun kayıt). Bahşiş oranının paydası `fare_amount`, ortalama toplam tutarın kaynağı `total_amount`.

Dört ana özet ayrı ayrı **17.210.961** kayda, ödeme payları yuvarlama öncesinde %100'e ulaşır. Bulgular temiz veride gözlenen binişleri anlatır. Yoğunluğun nedeni, kişi başına kullanım veya nakit bahşişler için ek veri gerekir.

**Yeniden üretim.** Temiz Parquet verisini §0'daki `python -m src.fetch_data` ile edin. Ardından proje kökünde çalıştırın. İlk komut, eksikse yalnızca `data/raw/taxi_zone_lookup.csv` dosyasını indirir. `python -m src.ingest` çalıştırıldıysa bölge tablosu ham veriyle birlikte zaten indirilir.

```powershell
python -c "from src.config import ZONE_LOOKUP_URL, ZONE_LOOKUP_PATH; from src.ingest import ensure_file; ensure_file(ZONE_LOOKUP_URL, ZONE_LOOKUP_PATH)"
python -m src.aggregate_trips --analysis all --export
```

Komut `reports/results/s3_*.csv` tablolarını üretir. Ders notebook’u altı özeti temiz Parquet verisinden PySpark ile hesaplayıp kapsam ve toplamları kontrol eder, ardından grafikleri oluşturur; `src/compare_aggregation.py` PySpark sonuçlarını hazırlayan yardımcı işlevleri de içerir. Notebook’taki süre deneyleri istenirse ayrı hücreden yeniden çalıştırılabilir. Kayıtlı çıktılar notebook açıldığında veri veya Spark kurulumu olmadan görüntülenebilir. Haritayı yeniden çizmek için [NYC Taxi Zones](https://catalog.data.gov/dataset/nyc-taxi-zones) GeoJSON dosyasını `data/raw/taxi_zones.geojson` olarak kaydedin. Gün × saat görseli [Loayib'in NYC Taxi analizinden](https://github.com/Loayib/NYC_Taxi_Analysis) esinlendi.

## 9. Öğrenci 4 — ML modeli: Yolculuk Süresi Tahmini

**Notebook:** [`notebooks/04_ml_duration_prediction.ipynb`](notebooks/04_ml_duration_prediction.ipynb) · **Ders gösterimi:** [`notebooks/04_ml_duration_ders.ipynb`](notebooks/04_ml_duration_ders.ipynb) · **Girdi:** 17.210.961 temiz yolculuktan 100K / 1M / 5M örneklem + verinin tamamı (17M)

Yolculuk süresi (`trip_duration_min`) için PySpark MLlib (`LinearRegression`, `GBTRegressor`) ve scikit-learn (`LinearRegression`, `HistGradientBoostingRegressor`) **aynı koşullarda** karşılaştırıldı:

- **Aynı veri:** örneklem ve train/test ayrımı satır hash'iyle bir kez üretilip diske yazılır; iki çerçeve aynı satırları kullanır.
- **Aynı özellikler:** 8 sayısal kolon + biniş/iniş bölgesi. Doğrusal modellerde bölge one-hot, ağaç modellerinde kategorik özellik.
- **Aynı hiperparametreler:** 50 ağaç, derinlik 5, öğrenme oranı 0,1.
- **Doğru ölçüm:** her ölçüm ayrı süreçte; bellek, süreç ağacının (Spark'ta JVM dahil) tepe RSS'i.

| İş | Çıktı | Teknik ayrıntı |
|---|---|---|
| Pipeline + deney | `src/ml_duration.py` | Bölge kodlaması yalnızca eğitim kümesinden öğrenilir (en sık 254 bölge + "diğer"). Spark'ta nominal metadata ile kategorik ağaç bölmesi (`maxBins=256`). |
| Ek analizler | `src/ml_analysis.py` | Özellik sızıntısı (biniş anında bilinmeyen ücret kalemleri çıkarılır), doğrulama kümesiyle hiperparametre araması (test seçimde kullanılmaz), mesafe/saat/ilçe bazında hata analizi (bölge tablosuyla join) |
| Bellek ölçümü | `src/mem_probe.py` | `PeakRSS`: Python + JVM süreç ağacını 50 ms'de bir örnekler (S5 de kullanır) |
| Sonuçlar | [`s4_comparison.csv`](reports/results/s4_comparison.csv), [`s4_feature_importance.csv`](reports/results/s4_feature_importance.csv), [`s4_pred_sample.csv`](reports/results/s4_pred_sample.csv), [`s4_ablation.csv`](reports/results/s4_ablation.csv), [`s4_tuning.csv`](reports/results/s4_tuning.csv), [`s4_error_analysis.csv`](reports/results/s4_error_analysis.csv) | 2 çerçeve × 2 model × 4 boyut = 16 ölçüm + ek analizler |
| Grafikler | [Şekil 18](reports/figures/18_s4_scalability.png), [19](reports/figures/19_s4_model_comparison.png), [20](reports/figures/20_s4_feature_actual_pred.png), [24](reports/figures/24_s4_ablation_tuning.png), [25](reports/figures/25_s4_error_analysis.png) | Süre + bellek; RMSE + R²; özellik önemi + gerçek vs tahmin; sızıntı + arama; hata analizi |

**Sonuçlar (5M ve tam veri 17M; i7-13700H, 32 GB RAM, performans modu; Spark verisi 20 bölüme dağıtılmış):**

| Çerçeve | Model | RMSE 5M | RMSE 17M | R² 17M | Eğitim 5M (sn) | Eğitim 17M (sn) | Tepe RSS 17M (MB) |
|---|---|---:|---:|---:|---:|---:|---:|
| PySpark | LinearRegression | 7,175 | 7,163 | 0,649 | 6,0 | **9,9** | 5.651 |
| scikit-learn | LinearRegression | 7,176 | 7,164 | 0,649 | 32,1 | 106,9 | 10.088 |
| PySpark | GBTRegressor | 5,469 | **5,453** | **0,796** | 140,7 | 269,1 | 6.918 |
| scikit-learn | HistGradientBoosting | 5,561 | 5,540 | 0,790 | 4,8 | **19,9** | 7.133 |

**Ek analizler (5M test, sklearn HistGradientBoosting):** ücret kalemleri çıkarılınca RMSE 5,561 → 5,536 (sızıntı endişesi doğruluğa mal olmuyor); mesafe de çıkarılınca 6,912. Doğrulama kümesiyle seçilen ayar (200 ağaç, sınırsız derinlik/63 yaprak) RMSE'yi **5,536 → 5,042** (R² 0,790 → 0,826) indiriyor. Hata mesafeyle artıyor ama göreli hata azalıyor (%29 → %16); ortalama sapma ≈ 0.

- **Karşılaştırma adil:** OLS iki çerçevede aynı sonucu veriyor (dört boyutta RMSE farkı ≤ 0,002). Farklar yalnızca algoritma uygulamasından geliyor.
- **GBT, doğrusal modelden ~%24 daha iyi** (RMSE 7,16 → 5,45). Doğrusal modelin R²'si boyutla sabit (0,64–0,65); GBT veri arttıkça iyileşiyor (0,76 → 0,80) ama 5M → 17M kazancı 0,001. Bu noktada veri eklemek değil, modeli büyütmek (arama: R² 0,826) kazandırıyor.
- **Özellik önemi:** `trip_distance` 0,47 · `DOLocationID` 0,18 · `pickup_hour` 0,13 · `PULocationID` 0,13 · `pickup_dow` 0,07.
- **Spark ve dosya düzeni:** örneklemler tek Parquet dosyası (1–2 row group) → Spark 1–2 çekirdekle eğitiyordu. Veriyi 20 bölüme dağıtmak büyük veride kazandırıyor (17M GBT 990 → 269 sn, LinearRegression 34 → 10 sn), küçük veride kaybettiriyor (100K GBT 23 → 103 sn: 50 ağaç × seviye başına 20 görevin koordinasyon maliyeti).
- **Ağaç modelinde klasik yaklaşım daha hızlı, fark kapanıyor:** sklearn GBT 100K'da ~500×, 5M'de ~29×, 17M'de ~14× hızlı. Ama marjinal maliyet de sklearn'de ~9× düşük (ek milyon kayıt: 1,2 sn vs 10,5 sn) → tek makinede Spark GBT yetişemiyor.
- **Doğrusal modelde klasik yaklaşım 1M'den itibaren geride, 17M'de 11× yavaş ve 10,1 GB bellek** (Spark 5,7 GB). Spark normal denklemi 20 görevde paralel kuruyor; sklearn seyrek one-hot matris + tek çekirdekli `lsqr` çözücü kullanıyor.
- **Bellek:** sklearn belleği kayıtla doğrusal artıyor; 17M ölçümü bu eğimi doğruladı (~0,40–0,58 KB/kayıt) → bu makinede ~55–80M kayıtta RAM dolar.
- **Önceki sürümlerdeki ölçüm hataları düzeltildi:** sklearn tüm veriyi okuyordu; Spark'ta yalnızca Python süreci ölçülüyordu; özellik setleri farklıydı; Spark 1–2 çekirdek kullanıyordu. Ayrıntı: [teknik rapor](docs/technical_report_s4.md).

**Yeniden üretim:**

```powershell
python -m src.fetch_data                 # veri yoksa indir
python -m src.ml_duration                # örneklemler + 16 ölçüm (~20 dk, performans modunda) + grafikler
python -m src.ml_duration --plots-only   # kayıtlı CSV'lerden grafikleri yeniden üret
python -m src.ml_analysis                # sızıntı + hiperparametre araması + hata analizi (~2 dk) + Şekil 24–25
python -m pytest tests/test_ml_duration.py tests/test_ml_analysis.py -v
```

Ayrıntılı teknik yorum: [`docs/technical_report_s4.md`](docs/technical_report_s4.md) · Sınav soruları: [`docs/exam_questions_s4.md`](docs/exam_questions_s4.md)

## 10. Öğrenci 5 — İşleme performansı ve dashboard

**Notebook:** [`notebooks/05_processing_benchmark.ipynb`](notebooks/05_processing_benchmark.ipynb) · **Ders gösterimi:** [`notebooks/05_processing_ders.ipynb`](notebooks/05_processing_ders.ipynb) · **Dashboard:** [`reports/dashboard/index.html`](reports/dashboard/index.html) (tarayıcıda açılır; sunucu ve ek kütüphane gerektirmez) · **Girdi:** S1'in boyut setleri (100K … 19M) + sentetik 57M (19M × 3)

Öğrenci 1 okumayı ölçtü; burada **işleme** ölçüldü. Üç ileri analitik işlem üç çalışma modeliyle (pandas, dosyadan okuyan Spark, önbelleğe alan Spark), CSV ve Parquet üzerinde, 6 veri boyutunda karşılaştırıldı:

- **groupby:** saate göre özet
- **join:** bölge tablosu → ilçe
- **window:** bölge başına en yüksek tutarlı 3 yolculuk

| İş | Dosya | Teknik ayrıntı |
|---|---|---|
| Benchmark | `src/benchmark_processing.py` | Her motor × boyut × format ayrı süreçte; 1 ısınma + 3 tekrar. pandas: bir kez yükle, bellekte işle. Spark: her işlem dosyadan (tembel), `collect` ile tetiklenir. Spark + cache: bir kez oku, 20 bölüme dağıt, önbelleğe al, işlemleri önbellekten çalıştır. Oturum açılışı ayrı ölçülür. Tepe RSS (JVM dahil). Sonuçlar checksum ile doğrulanır (tüm boyutlarda tutarlı). |
| Sentetik ölçek + düzen kontrolü | `src/scale_synthetic.py` | 19M × 3 = 57M kayıt (kopyalar blok blok yazılır, sıkıştırma yapay olarak iyileşmez). 19M Parquet'in 23 row group'lu kopyası (dosya düzeninin Spark'a etkisi). |
| Grafikler | `src/plots_processing.py` | [Şekil 21](reports/figures/21_s5_op_time.png) işlem süresi, [22](reports/figures/22_s5_end_to_end.png) uçtan uca + kesişim noktası, [23](reports/figures/23_s5_memory.png) tepe bellek, [26](reports/figures/26_s5_time_breakdown.png) sürenin adımlara dökümü |
| Dashboard | `src/dashboard.py` | Tüm öğrencilerin `reports/results/*.csv` sonuçlarını tek sayfada toplar. S5 grafiği etkileşimli: işlem ve metrik seçimi, değer ipucu, tablo görünümü. |
| Sonuçlar | [`s5_processing.csv`](reports/results/s5_processing.csv), [`s5_processing_raw.csv`](reports/results/s5_processing_raw.csv), [`s5_synthetic_manifest.csv`](reports/results/s5_synthetic_manifest.csv) | süre ort. ± std, throughput, tepe RSS, sonuç checksum'ı |

**Sonuçlar (19M kayıt, sn; i7-13700H, 20 iş parçacığı, 32 GB RAM, performans modu):**

| | pandas + CSV | pandas + Parquet | Spark + CSV | Spark + Parquet | Spark + cache + CSV | Spark + cache + Parquet |
|---|---:|---:|---:|---:|---:|---:|
| Hazırlık (yükleme / açılış / açılış + önbellek) | 18,8 | 0,4 | 5,5 | 5,8 | 17,4 | 14,6 |
| groupby | 0,70 | 0,70 | 5,86 | 0,91 | **0,56** | 0,69 |
| join | 1,97 | 2,13 | 4,20 | 0,71 | **0,47** | 0,61 |
| window | **2,07** | 2,09 | 6,62 | 6,10 | 3,34 | 3,33 |
| **Uçtan uca** | 23,6 | **5,3** | 22,2 | 13,5 | 21,7 | 19,3 |
| Tepe bellek | 2,7 GB | 3,1 GB | 4,9 GB | 4,2 GB | 6,4 GB | 5,7 GB |

**57M (sentetik):** uçtan uca pandas + Parquet **21,4 sn**, pandas + CSV 70,8 sn, Spark + cache + Parquet 36,8 sn, Spark + cache + CSV 42,7 sn. join pandas 8,5 sn vs Spark + cache 0,9 sn. Tepe bellek pandas 9,1 GB vs Spark + cache 6,8 GB.

- **pandas + CSV, ~16M kayıtta Spark'ın gerisine düşüyor.** Darboğaz işlem değil, tek çekirdekli CSV parse (~1M kayıt/sn); 57M'de 70,8 sn vs 42,7 sn.
- **Format, motordan daha belirleyici:** pandas + Parquet 57M'de bile uçtan uca en hızlı seçenek. Ama süresi veriden hızlı artıyor (3× veri → 4× süre; Spark + cache 1,9×).
- **İşlem türü sonucu değiştiriyor:** join'de Spark 4,5M'den itibaren önde (broadcast hash join; 57M'de 8–9×). window'da pandas 57M'e kadar önde ya da başa baş (shuffle + sıralama maliyeti).
- **Dosya düzeni Spark'ı kısıtlıyor:** S1'in tek dosyalık Parquet'leri 1–3 row group → Spark 1–3 çekirdekle okuyor. Aynı 19M'i 23 row group'la yazınca Spark window 6,1 → 3,85 sn.
- **Önbellek** işlemleri hızlandırıyor (19M join 0,47 sn) ama yükleme maliyeti (Parquet 8,9 sn) yalnızca tekrarlanan sorgularda amorti oluyor.
- **Sert sınır bellek:** pandas'ın tepe belleği 6 kolonla ~0,15 KB/kayıt doğrusal artıyor (57M ölçümü doğruladı); 57M'de pandas ilk kez Spark'tan fazla bellek kullanıyor. Bu makinede ~200M kayıtta RAM dolar (20 kolonla ~65M).

**Yeniden üretim:**

```powershell
python -m src.build_sizes                     # boyut setleri yoksa (S1)
python -m src.benchmark_processing            # ~15 dk; tamamlananları atlar, --force ile yeniden ölçer
python -m src.scale_synthetic --factor 3      # sentetik 57M
python -m src.benchmark_processing --sizes x3 --engines pandas spark_cache
python -m src.scale_synthetic --layout        # 19M Parquet, 23 row group
python -m src.benchmark_processing --sizes full_rg16 --formats parquet_snappy
python -m src.plots_processing
python -m src.dashboard                       # reports/dashboard/index.html
python -m pytest tests/test_benchmark_processing.py tests/test_plots_processing.py tests/test_dashboard.py tests/test_scale_synthetic.py -v
```

Ayrıntılı teknik yorum: [`docs/technical_report_s5.md`](docs/technical_report_s5.md) · Sınav soruları: [`docs/exam_questions_s5.md`](docs/exam_questions_s5.md)

---

## Belgeler
- [Kısa teknik rapor — Öğrenci 1 ve 2](docs/technical_report_s1_s2.md)
- [Kısa teknik rapor — Öğrenci 3](docs/technical_report_s3.md)
- [Kısa teknik rapor — Öğrenci 4](docs/technical_report_s4.md)
- [Kısa teknik rapor — Öğrenci 5](docs/technical_report_s5.md)
- [Sınav soruları — Öğrenci 1 ve 2](docs/exam_questions_s1_s2.md)
- [Sınav soruları — Öğrenci 3](docs/exam_questions_s3.md)
- [Sınav soruları — Öğrenci 4](docs/exam_questions_s4.md)
- [Sınav soruları — Öğrenci 5](docs/exam_questions_s5.md)
- [Sonuç dashboard'u](reports/dashboard/index.html)
- [Yol haritası ve kararlar](docs/ROADMAP.md) · [Proje gereksinimleri](docs/PROJECT_REQUIREMENTS.md)
