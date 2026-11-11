# NYC Taxi Büyük Veri Analitiği — Yol Haritası (Öğrenci 1 & 2)

> **Proje:** Milyonlarca taksi yolculuğundan şehir içi ulaşım davranışlarını analiz etmek.
> **Araştırma sorusu:** Veri büyüdükçe klasik analiz yaklaşımı ne zaman yetersiz kalıyor?
> **Bu dokümanın kapsamı:** Öğrenci 1 ve Öğrenci 2 görevleri (tek kişi tarafından yürütülüyor) + ortak teslimlere katkı.
> **Resmi gereksinimler ve puanlama:** [PROJECT_REQUIREMENTS.md](PROJECT_REQUIREMENTS.md) — her adım buna göre kontrol edilir.

---

## 1. Görev Tanımları (resmi metin → somut karşılık)

| | Resmi görev metni | Somut olarak ne demek |
|---|---|---|
| **Öğrenci 1** | NYC Taxi verisini temin eder, veri sözlüğünü hazırlar, CSV/Parquet dönüşümünü ve veri büyüklüğü analizini yapar. | (a) Veriyi resmi kaynaktan **script ile** indirir, (b) kolon kolon açıklamalı veri sözlüğü yazar, (c) 3+ boyutta veri seti üretir ve her birini CSV + Parquet olarak kaydeder, (d) format × boyut için disk, okuma süresi ve bellek ölçer, yorumlar. |
| **Öğrenci 2** | Eksik/hatalı kayıtları temizler; anormal mesafe, ücret ve süre değerlerini analiz eder. | (a) Veriyi profiller (eksik/aykırı oranları), (b) kural bazlı temizlik yapar ve her kuralın etkisini sayar, (c) mesafe/ücret/süre için istatistiksel anomali analizi yapar, (d) temiz veriyi diğer öğrencilerin kullanacağı standart formatta teslim eder. |

### Kapsam dışı (başka öğrencilerin işi — çakışma olmasın)
- Saat/gün/bölge/ödeme türü **aggregation** analizleri → Öğrenci 3
- ML modeli → Öğrenci 4
- **Pandas vs Spark işleme** performansı ve dashboard → Öğrenci 5
  - Sınır: Öğrenci 1 **depolama & okuma** tarafını ölçer (disk boyutu, okuma süresi, bellek). Öğrenci 5 **işleme** (groupby/join süresi, Pandas vs Spark) tarafını ölçer. Öğrenci 5 isterse Öğrenci 1'in ürettiği boyut setlerini kullanır.

---

## 2. Veri Kaynağı

- **Kaynak:** NYC TLC Trip Record Data — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Kullanılacak set:** Yellow Taxi Trip Records, **2026 Ocak–Mayıs** → **18.999.282 kayıt**, 5 dosya, 310 MB (gerçek veri; sentetik çoğaltma gerekmez)
- **Şema:** 5 dosyada da aynı, 20 kolon. 5 Ocak 2025'te başlayan Manhattan tıkanıklık ücreti nedeniyle `cbd_congestion_fee` kolonu var. Veride kolon adı `Airport_fee` (sözlükte `airport_fee`).
- **TLC dosyaları ZSTD ile sıkıştırılmış** (snappy değil), her dosya 4 row group.
- **Yardımcı dosyalar** (`data/raw/` içinde, elle indirildi):
  - `data_dictionary_trip_records_yellow.pdf` — resmi sözlük (18 Mart 2025 sürümü)
  - `taxi_zone_lookup.csv` — 265 bölge; 264 = Unknown, 265 = Outside of NYC
  - `taxi_zones/` shapefile + 5 ilçe haritası (JPG) — S1/S2 kullanmaz; S3/S5 harita için kullanabilir
  - `trip_record_user_guide.pdf`, `working_parquet_format.pdf` — arka plan
- **Not:** TLC 2022'den beri yalnızca Parquet yayınlıyor → CSV formatı bizim tarafımızdan üretilir.
- **Kapsam kararı:** Green / FHV / HVFHV setleri **kullanılmaz** (farklı şema, ek değer düşük).

### İlk tarama bulguları (Ocak 2026)
- **%29,2 kayıtta (1.088.058) 5 kolon birlikte null:** `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`. Bu satırların **tamamı `payment_type = 0` (Flex Fare)** → hata değil, ön fiyatlı yolculuk. **Silinmez, `is_flex_fare` ile işaretlenir** (bkz. R1/R7).
- `RatecodeID = 99` → 110.864 kayıt. VendorID değerleri: 1, 2, 6, 7.
- Ay dışı pickup tarihleri var (2025-12-31, 2026-02-01) → R6 gerekli.

---

## 3. Teknoloji ve Ortam

| Bileşen | Seçim | Not |
|---|---|---|
| Dil | Python 3.11 | kurulu |
| Ölçeklenebilir işleme | **PySpark 3.5.9** | en yaygın/stabil sürüm |
| Java | **Temurin JDK 17** (taşınabilir) | `C:\tools\jdk-17` — sistemdeki Java 21'e dokunulmadı |
| Hadoop (Windows) | `winutils.exe` + `hadoop.dll` (3.3.6) | `C:\tools\hadoop\bin` — Spark'ın dosya yazabilmesi için |
| Klasik yaklaşım | pandas **2.3** + pyarrow | pandas 3, PySpark 3.5 ile uyumsuz olabilir → 2.x'e sabitlendi |
| Görselleştirme | matplotlib / seaborn | |
| Ortam | `.venv` + `requirements.txt` (sürümler sabit) | |

**Kurulum kararları (Faz 0'da test edildi, hepsi geçti):**
- Proje yolu Türkçe karakter içermeyen `C:\Users\ismai\Desktop\NYC Taxi` klasörüne taşındı.
- Bilgisayar adı `Ismail_Esat` alt çizgi içerdiği için Spark URL'si geçersiz oluyordu → `src/spark_session.py` içinde `SPARK_LOCAL_HOSTNAME=localhost` ve `spark.driver.host=localhost` ayarlandı.
- Tüm ortam ayarları (JAVA_HOME, HADOOP_HOME, PYSPARK_PYTHON, driver belleği 6g) tek yerde: `src/spark_session.py` → her yerde `get_spark()` kullanılır.
- Makine: 16 GB RAM, 28 çekirdek, C: diskinde ~30 GB boş alan.
- Kurulum testi: `scripts/smoke_test_spark.py` (Parquet/CSV yaz-oku, Python UDF, Arrow toPandas).

### Klasör yapısı
```
NYC Taxi/
├── data/                  # .gitignore'da (veri Git'e girmez)
│   ├── raw/               # TLC'den indirilen orijinal parquet + zone lookup
│   ├── processed/         # KULLANILMIYOR → boyut setleri D:\BVAVeri\processed\ (C: dolu; NYC_DATA_ROOT)
│   └── clean/             # Öğrenci 2 çıktısı → Öğrenci 3/4/5 girdisi
├── scripts/
│   └── smoke_test_spark.py  # kurulum testi
├── src/
│   ├── spark_session.py   # get_spark(): Windows ortam ayarları + SparkSession
│   ├── config.py          # yollar, aylar, eşik değerleri (sabitler)
│   ├── ingest.py          # S1: indirme
│   ├── build_sizes.py     # S1: örnekleme + CSV/Parquet yazma
│   ├── benchmark_io.py    # S1: disk / okuma / bellek ölçümü
│   ├── profile_data.py    # S2: profilleme
│   └── clean.py           # S2: temizlik pipeline'ı
├── notebooks/
│   ├── 01_data_acquisition_and_formats.ipynb   # S1 anlatım + grafikler
│   └── 02_cleaning_and_anomalies.ipynb         # S2 anlatım + grafikler
├── reports/
│   ├── figures/
│   └── results/           # ölçüm sonuçları (csv/json)
├── docs/
│   ├── ROADMAP.md
│   ├── data_dictionary.md
│   ├── cleaning_rules.md
│   └── architecture.png
├── README.md
└── requirements.txt
```

---

## 4. Fazlar ve Görevler

### Faz 0 — Altyapı ✅
- [x] **T0.1** Git deposu + `.gitignore` (`data/`, `.venv`, `.ipynb_checkpoints`, `__pycache__`)
- [x] **T0.2** Klasör yapısı + `requirements.txt`
- [x] **T0.3** PySpark 3.5.9 + JDK 17 + winutils; duman testi geçti (yaz/oku/UDF/toPandas)
- **Bitti kriteri:** Spark Windows'ta hatasız okuma/yazma yapıyor. ✔

---

### Faz 1 — ÖĞRENCİ 1: Veri Temini, Sözlük, Format Dönüşümü, Boyut Analizi

#### T1.1 Veri temini (`src/ingest.py`)
- [x] 2026-01 … 2026-05 Yellow Taxi parquet + zone lookup CSV + yardımcı dosyalar elle indirildi
- [x] Script: eksik dosyayı indir, var olanı atla; indirme hatasında anlamlı hata ver (tekrar üretilebilirlik)
- [x] Doğrulama: her dosya açılabiliyor mu, şema beklenen 20 kolon mu, kayıt sayısı + boyut logla
- Çalıştırma: `python -m src.ingest` · Testler: `python -m pytest tests` (10 test)
- **Çıktı:** `data/raw/*.parquet`, `data/raw/taxi_zone_lookup.csv`

#### T1.2 Veri sözlüğü (`docs/data_dictionary.md`)
- [x] 20 kolonun her biri için (`cbd_congestion_fee` dahil): **ad, veri tipi (gerçek şemadan), açıklama, birim, geçerli aralık / izinli değerler, örnek değer, eksik oranı**
- [x] Kod tabloları: `VendorID`, `RatecodeID`, `payment_type`, `store_and_fwd_flag`
- [x] Zone lookup tablosunun açıklaması ve `PULocationID/DOLocationID` ile ilişkisi
- [x] Flex Fare açıklaması: `payment_type = 0` ↔ 5 kolonun birlikte null olması (19M kayıtta %100 örtüşme)
- [x] Türetilecek kolonlar (S2/S3/S4 için): `trip_duration_min`, `avg_speed_mph`, `pickup_hour`, `pickup_dow`, `is_flex_fare`
- **Kaynak:** TLC Data Dictionary PDF + gerçek veriden okunan şema/istatistikler
- Çalıştırma: `python -m src.dictionary_stats` → `reports/results/{dictionary_stats,code_value_counts,flex_fare_check}.csv`

#### T1.3 Boyut setleri ve format dönüşümü (`src/build_sizes.py`)
- [x] 5 ay birleştirilir → **tam set (18.999.282)**
- [x] Örnekleme: **100K, 1M, 5M, 10M** (+ full) — `rand(seed)` yerine **satır hash'i** (xxhash64): makineden bağımsız ve **iç içe** setler (100K ⊂ 1M ⊂ … ⊂ full); satır sayıları ±%0,2
- [x] Her boyut × format **tek dosya** (`coalesce(1)`): pandas tek çağrıyla okur; tüm codec'ler aynı satır sırasını görür; Parquet row group hedefi 128 MB
- [x] Her boyut için yazılan formatlar:
  - CSV (sıkıştırmasız)
  - Parquet (snappy — Spark varsayılanı)
  - Parquet (gzip) — yüksek sıkıştırma
  - Parquet (zstd) — TLC'nin kullandığı codec
- **Çıktı:** `D:\BVAVeri\processed\size_{100k,1m,5m,10m,full}\{csv,parquet_snappy,parquet_gzip,parquet_zstd}\yellow.{csv,parquet}` (toplam 5,2 GB) + `reports/results/size_sets_manifest.csv`
- Çalıştırma: `python -m src.build_sizes [--sizes ...] [--force]` (var olanı atlar). Kök klasör `NYC_DATA_ROOT` ile değiştirilir.

| Boyut | Satır | CSV | snappy | gzip | zstd | CSV / zstd |
|---|---|---|---|---|---|---|
| 100k | 99.839 | 10,0 MB | 2,3 MB | 1,7 MB | 1,7 MB | 5,9× |
| 1m | 1.000.051 | 100 MB | 21,9 MB | 16,4 MB | 16,0 MB | 6,3× |
| 5m | 4.997.943 | 502 MB | 106 MB | 81,9 MB | 79,9 MB | 6,3× |
| 10m | 9.996.122 | 1.004 MB | 206 MB | 160 MB | 157 MB | 6,4× |
| full | 18.999.282 | 1.907 MB | 392 MB | 296 MB | 290 MB | 6,6× |

- Not (T1.4 için): tek dosyada row group sayısı 1–4 → Spark tek Parquet dosyasını en fazla row group sayısı kadar paralel okur; 28 çekirdeğin çoğu boşta kalabilir. Bu bir **bulgu** olarak raporlanır.

#### T1.4 Veri büyüklüğü analizi (`src/benchmark_io.py`)
Her **boyut × format** için ölçülenler:
- [x] Disk boyutu (MB) — manifest'ten
- [x] Tam okuma süresi — pandas ve Spark. Spark `noop` yazımıyla tetiklenir (**`count()` değil**: Parquet'te metadata'dan cevap verir, veriyi okumaz)
- [x] Kolon seçerek okuma süresi (3 kolon: pickup zamanı, mesafe, ücret) — column pruning etkisi
- [x] pandas bellek: tablo boyutu (`memory_usage(deep=True)`, süre dışında) + **okuma sırasındaki tepe bellek** (peak working set, her ölçüm ayrı süreçte)
- [x] Her ölçüm **1 ısınma + 3 tekrar**, ortalama + std; sıcak işletim sistemi önbelleği koşulu
- [x] Spark CSV ayrıca `inferSchema` ile → şemasız formatın maliyeti
- [x] Her okumanın satır sayısı manifest ile doğrulandı (88 kombinasyon, 0 hata)
- **Adillik:** her okuma aynı tipli tabloyu üretir (CSV'de tarih parse; Spark CSV'ye Parquet şeması verilir). Spark oturum açılış süresi ölçüme dahil değildir.
- **Çıktı:** `reports/results/io_benchmark.csv` (özet), `io_benchmark_raw.csv` (her tekrar)
- Çalıştırma: `python -m src.benchmark_io [--sizes ...] [--engines ...]` (biten kombinasyonları atlar)

**Ana bulgular (full set, 19M kayıt):**

| | Disk | Okuma (tüm kolonlar) | Okuma (3 kolon) | pandas tepe bellek |
|---|---|---|---|---|
| pandas + CSV | 1.907 MB | **71,1 sn** | 35,5 sn | **9.437 MB (RAM'in %59'u)** |
| pandas + Parquet zstd | 290 MB | 2,0 sn | 0,9 sn | 6.586 MB (%41) |
| pandas + Parquet snappy | 392 MB | 1,8 sn | 0,6 sn | 5.815 MB (%36) |
| Spark + CSV (şema verildi) | 1.907 MB | 8,1 sn | 4,5 sn | — |
| Spark + CSV (`inferSchema`) | 1.907 MB | 15,7 sn | — | — |
| Spark + Parquet snappy | 392 MB | 3,7 sn | 1,3 sn | — |

1. **Klasik yaklaşım = pandas + CSV** yetersiz kalan kombinasyon: 19M'de 71 sn ve RAM'in %59'u. Tepe bellek ≈ satır başına 0,5 KB → doğrusal ekstrapolasyonla **~32M kayıtta 16 GB RAM dolar** (pratikte işletim sistemi payıyla ~25M).
2. **Bellek şişmesi:** CSV diskte 1,9 GB → bellekte tablo 3,7 GB → okuma tepesi 9,4 GB (disk boyutunun ~5 katı).
3. **Format, motordan daha belirleyici:** pandas + Parquet (1,8 sn), Spark + CSV'den (8,1 sn) hızlı. Tek makinede pyarrow'un çok iş parçacıklı C++ okuyucusu Spark'ın JVM yükünü geçiyor.
4. **Column pruning:** Parquet'te 3 kolon okumak 3 kat hızlı; CSV'de yalnızca ~2 kat (metin satırının tamamı yine okunmak zorunda).
5. **Şemasızlığın maliyeti:** Spark CSV'de `inferSchema` okuma süresini ~2 katına çıkarır (ek tam tarama).
6. **Codec:** gzip en küçük ama en yavaş açılan; zstd ≈ gzip boyutunda, snappy hızına yakın.
7. **Paralellik:** Spark tek Parquet dosyasını byte aralıklarına göre 4–28 partition'a böler, ama veriyi yalnızca row group sayısı (1–4) kadar görev okur → planlanan partition ≠ gerçek paralellik.

#### T1.5 Görselleştirme ve yorum (`notebooks/01_...ipynb`)
- [ ] Grafik: format başına disk boyutu (boyutlara göre gruplu bar)
- [ ] Grafik: okuma süresi vs veri boyutu (log ölçek çizgi grafik)
- [ ] Grafik: pandas bellek kullanımı vs veri boyutu
- [ ] **"Neden" yorumu:** kolon bazlı depolama, sıkıştırma + encoding (dictionary/RLE), gömülü şema (tip çıkarımı yok), column pruning, CSV'nin metin olarak parse maliyeti

---

### Faz 2 — ÖĞRENCİ 2: Temizlik ve Anomali Analizi (PySpark)

#### T2.1 Profilleme (`src/profile_data.py`)
- [ ] Kolon başına: null sayısı/oranı, min, max, mean, p1/p25/p50/p75/p99/p99.9
- [ ] Mantıksal tutarsızlık sayıları (negatif ücret, sıfır mesafe, dropoff < pickup, ay dışı tarih…)
- **Çıktı:** `reports/results/profile_raw.csv`

#### T2.2 Temizlik kuralları (`src/clean.py`, `docs/cleaning_rules.md`)
Eşikler `config.py`'de sabit olarak tutulur. Her kural için **kaç kaydı etkilediği ayrı sayılır**.

| # | Kural | Gerekçe |
|---|---|---|
| R1 | `passenger_count` 0 veya > 6 (**null ise silinmez** — Flex Fare, `is_flex_fare` ile işaretlenir) | fiziksel olarak geçersiz |
| R2 | `trip_distance` ≤ 0 veya > 100 mil | ölçüm hatası / şehir dışı uç değer |
| R3 | `fare_amount` ≤ 0 veya `total_amount` ≤ 0 | iade/iptal kayıtları |
| R4 | süre ≤ 1 dk veya > 180 dk | taksimetre hatası, kapatılmamış yolculuk |
| R5 | ortalama hız > 80 mph | fiziksel olarak imkânsız (mesafe/süre tutarsızlığı) |
| R6 | pickup tarihi seçilen ayların (2026-01..05) dışında | hatalı zaman damgası (eski/gelecek tarihli kayıtlar) |
| R7 | `RatecodeID` = 99 (**null ise silinmez** — Flex Fare) | sözlükte "Null/unknown" kodu |
| R8 | `PULocationID` / `DOLocationID` ∈ {264, 265} | bilinmeyen bölge |
| R9 | tam tekrar eden (duplicate) kayıtlar | mükerrer kayıt |
| R10 | `cbd_congestion_fee` < 0 veya null | yeni kolon tutarlılığı (2025) |

- [ ] Eksik değer stratejisi: silme mi doldurma mı → kolon bazında karar ve gerekçe (Flex Fare null'ları silinmez)
- [ ] Kural etkisi tablosu: her kural için **tek başına** etkilediği kayıt + **sırayla uygulandığında** sildiği kayıt (kurallar örtüşür)
- [ ] "Naif temizlik" karşılaştırması: null içeren her satırı silmek verinin %25,3'ünü (4,8M kayıt) yok ederdi → rapora bulgu olarak
- [ ] R9 duplicate tespiti **window** fonksiyonuyla (`row_number() over (partition by tüm kolonlar)`) → hangi kopyanın tutulduğu açık
- [ ] Temizlik öncesi/sonrası özet istatistik karşılaştırması (ortalama ücret, süre, mesafe ne kadar değişti?)

#### T2.3 Anomali analizi (mesafe, ücret, süre)
- [ ] **Kural bazlı** (T2.2) vs **istatistiksel** (IQR ve p99.9 eşiği) yöntemlerin karşılaştırılması — hangisi kaç kayıt işaretliyor, ne kadar örtüşüyor
- [ ] Ücret–mesafe tutarlılığı: mesafeye göre beklenmeyen ücretler (örn. kısa mesafe + çok yüksek ücret)
- [ ] Anomalilerin örüntüsü: hangi saat/vendor/bölgede yoğunlaşıyor? (kaynağı hakkında yorum) — zone lookup ile **join** (ilçe/bölge adı)
- [ ] Bağlama duyarlı anomali: **window** ile güzergâh/saat grubu içinde ücret ve süre yüzdelikleri (örn. aynı PU→DO bölge çiftinde p99 üstü) → global eşiğin kaçırdığı anomaliler
- **İleri analitik işlem şartı** (aggregation / join / window) S2 tarafında bu iki maddeyle karşılanır.

#### T2.4 Temiz veri teslimi (diğer öğrencilere arayüz)
- [ ] `data/clean/` → Parquet, `pickup_year/pickup_month` ile partition'lı
- [ ] Türetilmiş kolonlar eklenir: `trip_duration_min`, `avg_speed_mph`, `pickup_hour`, `pickup_dow`, `is_flex_fare`
- [ ] Şema notu `docs/data_dictionary.md` sonuna eklenir ("Clean schema" bölümü)
- **Bitti kriteri:** Öğrenci 3/4/5 tek satırla okuyabiliyor: `spark.read.parquet("data/clean")`

#### T2.5 Görselleştirme ve yorum (`notebooks/02_...ipynb`)
- [ ] Mesafe / ücret / süre: temizlik öncesi vs sonrası histogram veya boxplot (log ölçek)
- [ ] Kural başına silinen kayıt bar grafiği
- [ ] Mesafe–ücret scatter (örneklenmiş), anomaliler renkli
- [ ] **"Neden" yorumu:** hatalı kayıtların olası kaynakları (GPS/taksimetre hatası, iadeler, manuel giriş, saat dilimi/sistem hataları) ve temizliğin sonraki analizlere etkisi

---

### Faz 3 — Ortak Teslimlere Katkı
- [ ] **Mimari şema** (`docs/architecture.png`): TLC → raw Parquet → Spark (dönüşüm + temizlik) → clean Parquet → S3 aggregation / S4 ML / S5 benchmark + dashboard
- [ ] **README** bölümleri: veri kaynağı, kurulum, çalıştırma sırası, **Öğrenci 1 ve 2 teknik katkısı** (ayrı başlık)
- [ ] **Kısa teknik rapor — S1/S2 bölümü:** yöntem, ölçüm tabloları, grafikler, "neden" yorumu (notebook'lardan derlenir)
- [ ] **Sınav soruları:** projenin 10 sorusundan S1/S2 payı (5 seçenekli) — en az biri Parquet/format, biri veri temizliği üzerine
- [ ] **Araştırma sorusuna katkı paragrafı:** "Hangi boyutta, hangi formatta klasik yaklaşım (pandas + CSV) yetersiz kalmaya başladı ve neden?"

---

## 5. Değerlendirme Kriterleriyle Eşleşme

| Ölçüt (puan) | S1/S2'nin karşılığı |
|---|---|
| Big Data niteliği ve mimari tasarım (5) | 19M kayıt, 5 boyut seti, Spark pipeline, mimari şema |
| Çalışan teknik uygulama (7) | Script ile indirme → dönüşüm → temizlik; tekrar üretilebilir, `clean/` diğerlerini besliyor |
| Veri işleme ve analitik derinlik (5) | Profilleme, 10 temizlik kuralı, kural vs istatistiksel anomali karşılaştırması |
| Deney, karşılaştırma ve nicel sonuç (5) | Format × boyut ölçüm tablosu (3 tekrar), kural etki tablosu |
| 5 öğrencinin dengeli katkısı (5) | README'de ayrı katkı bölümü, net kapsam sınırları |
| Sunum ve sorulara hâkimiyet (3) | Her sonucun "neden" yorumu, sınav soruları |

## 6. Proje Şartları Kontrol Listesi (S1/S2 kapsamında)
- [x] Gerçek / açık kaynak veri → TLC (2026 Ocak–Mayıs, 19M kayıt; sentetik ölçekleme gerekmedi)
- [x] En az 3 veri büyüklüğü → 100K / 1M / 5M / 10M (+ full) üretildi (T1.3)
- [x] Ölçeklenebilir teknoloji → PySpark (sözlük istatistikleri, boyut setleri; temizlik de Spark ile)
- [x] En az 2 format → CSV vs Parquet (snappy, gzip, zstd) yazıldı (T1.3)
- [ ] İleri analitik işlem → join (zone lookup) + window (duplicate, bağlama duyarlı anomali) — T2.2/T2.3
- [x] Performans ölçümü → disk boyutu, okuma süresi, tepe bellek (T1.4, 5 boyut × 4 format × 2 motor, 3 tekrar)
- [ ] Görselleştirme → T1.5 + T2.5
- [ ] "Neden" yorumu → her notebook'ta
- [ ] Git ✔ + requirements.txt ✔ + README + mimari şema + kısa teknik rapor
- [ ] Teknik katkı ayrı gösterim → README'de S1 / S2 başlıkları
- [ ] Sınav soruları (5 seçenekli) → S1/S2 payı

## 7. Riskler
| Risk | Önlem |
|---|---|
| Spark Windows'ta yazma hatası (winutils) | ✔ Çözüldü (Faz 0) |
| Disk alanı (C: ~27 GB boş) | ✔ Çözüldü: boyut setleri (5,2 GB) ve Spark geçici dosyaları `D:\BVAVeri` (NVMe, 441 GB boş) |
| 10M/full sette pandas RAM yetersizliği | Bu bir **bulgu**dur — raporlanır (araştırma sorusunun cevabı); benchmark ayrı süreçte çalıştırılır ki çökme tüm ölçümü durdurmasın |
| Flex Fare null'larının yanlışlıkla silinmesi | R1/R7 null'ları hariç tutar; `is_flex_fare` kolonu |
| Diğer öğrencilerin S2 çıktısını beklemesi | T2.4 şeması erkenden paylaşılır; ilk ay için ön sürüm erken verilir |
