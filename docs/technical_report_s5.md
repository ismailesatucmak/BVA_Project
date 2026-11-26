# Kısa Teknik Rapor — Öğrenci 5

Öğrenci 1'in boyut setleri (100K / 1M / 5M / 10M / 19M kayıt; CSV ve Parquet snappy) ve bunlara ek olarak **sentetik 57M** (19M × 3) üzerinde üç ileri analitik işlemin **işleme** süresini ve belleğini ölçtüm. Karşılaştırdığım üç çalışma modeli: pandas, dosyadan okuyan PySpark ve önbelleğe alan PySpark. Bütün sonuçları tek bir dashboard'da topladım. Okuma ve depolama performansı Öğrenci 1'in alanıdır ([`io_benchmark.csv`](../reports/results/io_benchmark.csv)); burada tekrar ölçülmedi.

## Yöntem

| Bileşen | Seçim ve gerekçe |
|---|---|
| **İşlemler** | **groupby:** biniş saatine göre yolculuk sayısı, ortalama mesafe, ortalama ücret ve toplam gelir (24 grup). **join:** yolculuklar ⋈ TLC bölge tablosu (265 satır), ilçeye göre sayım ve ortalama tutar. **window:** her biniş bölgesinde en yüksek tutarlı 3 yolculuk; `row_number() over (partition by PULocationID order by total_amount desc)`. |
| **Girdi** | Ham veri boyut setleri (`build_sizes`; xxhash64 ile iç içe örneklem). İki motor da aynı 6 kolonu okur. CSV'de tarih parse edilir; Spark CSV'ye Parquet şeması verilir. |
| **Çalışma modelleri** | **pandas:** veri bir kez belleğe yüklenir (`load`), işlemler bellekteki tabloda çalışır. **Spark:** tembel değerlendirme nedeniyle her işlem dosyadan okuyarak çalışır; sonuç küçük olduğu için `toPandas()` ile tetiklenir. **Spark + cache:** pandas'ın "bir kez yükle" modelinin Spark karşılığı. Veri bir kez okunur, çekirdek sayısı kadar bölüme dağıtılır (`spread_partitions`) ve `cache()` + `count()` ile Spark belleğine alınır (`load`); işlemler önbellekten çalışır. Spark oturum açılışı (`startup`) ayrı ölçülür. |
| **Uçtan uca süre** | pandas = yükleme + 3 işlem. Spark = oturum açılışı + 3 işlem. Spark + cache = oturum açılışı + önbelleğe yükleme + 3 işlem. |
| **Sentetik ölçekleme** | `src/scale_synthetic.py`: 19M Parquet'in 3 kopyası **art arda bloklar hâlinde** (satır satır değil) yazılır → 56.997.846 kayıt. Satır satır çoğaltma Parquet'in RLE/dictionary kodlamasını yapay olarak iyileştirirdi; dosya boyutu oranı 2,999× ile bunun olmadığı doğrulandı. groupby/join checksum'ı 19M'nin tam 3 katı. window'da ilk 3 artık aynı yolculuğun 3 kopyasıdır (eşit tutarlar); sonuç yine motorlar arasında aynı. |
| **Dosya düzeni kontrolü** | Öğrenci 1'in Parquet dosyaları tek dosya, 128 MB row group: 5M = 1, 10M = 2, 19M = 3 row group. Spark bir row group'u bölemez → dosya 1–3 dolu bölümle okunur (ölçüldü: 19M'de 20 bölümün 3'ü dolu). Etkiyi ayırmak için aynı 19M satırların 16 MB row group'lu (23 row group) bir kopyası üretilip ayrıca ölçüldü. |
| **Tekrar** | Her işlem 1 ısınma + 3 tekrar (ortalama ± std). İşletim sistemi önbelleği sıcak. Spark oturum açılışı her süreçte bir kez (soğuk) ölçüldü; süreçler arası ortalama ± std raporlanır. |
| **İzolasyon** | Her motor × boyut × format ayrı alt süreçte çalıştı; motorlar aynı anda çalışmadı. |
| **Bellek** | Her adım boyunca süreç ağacının (Python + JVM) tepe RSS'i, `src/mem_probe.PeakRSS` ile 50 ms'de bir örneklendi. |
| **Doğruluk kontrolü** | Her işlemin sonucu satır sayısı + checksum ile kaydedildi. Her boyutta (57M ve düzen kopyası dahil) tüm motor × format kombinasyonları aynı sonucu verdi (`check_consistency`). |
| **Makine** | Intel i7-13700H (14 çekirdek / 20 iş parçacığı), 32 GB RAM, NVMe SSD, Windows 11. Spark `local[*]`, 6 GB sürücü belleği. Dizüstü fişte ve **performans modunda**; dengeli modda süreler ~2,7× uzuyor. Yeni ölçümlerden önce Spark 1M Parquet kontrolü önceki değerleri tekrarladı (açılış 5,48 vs 5,56 sn; işlemler ±%15). |

## Sonuçlar

Kaynak: [`s5_processing.csv`](../reports/results/s5_processing.csv) (özet) · [`s5_processing_raw.csv`](../reports/results/s5_processing_raw.csv) (her tekrar) · [`s5_synthetic_manifest.csv`](../reports/results/s5_synthetic_manifest.csv).

**19M kayıt, işlem süresi (sn, 3 tekrar ortalaması):**

| | pandas + CSV | pandas + Parquet | Spark + CSV | Spark + Parquet | Spark + cache + CSV | Spark + cache + Parquet |
|---|---:|---:|---:|---:|---:|---:|
| Hazırlık | 18,82 (yükleme) | 0,37 (yükleme) | 5,48 (açılış) | 5,81 (açılış) | 6,85 + 10,50 | 5,70 + 8,93 |
| groupby (saat) | 0,70 | 0,70 | 5,86 | 0,91 | **0,56** | 0,69 |
| join (ilçe) | 1,97 | 2,13 | 4,20 | 0,71 | **0,47** | 0,61 |
| window (bölge başına ilk 3) | **2,07** | 2,09 | 6,62 | 6,10 | 3,34 | 3,33 |
| **Uçtan uca** | 23,6 | **5,3** | 22,2 | 13,5 | 21,7 | 19,3 |
| Tepe bellek (GB) | 2,7 | 3,1 | 4,9 | 4,2 | 6,4 | 5,7 |

**Uçtan uca süre (sn) — boyuta göre:**

| | 100K | 1M | 5M | 10M | 19M | 57M (sentetik) |
|---|---:|---:|---:|---:|---:|---:|
| pandas + CSV | 0,16 | 1,34 | 6,58 | 12,73 | 23,56 | 70,75 |
| pandas + Parquet | 0,05 | 0,27 | 1,31 | 2,61 | **5,28** | **21,40** |
| Spark + CSV | 6,81 | 9,33 | 11,96 | 15,11 | 22,17 | — |
| Spark + Parquet | 6,45 | 7,23 | 10,56 | 11,89 | 13,53 | — |
| Spark + cache + CSV | 11,27 | 12,14 | 14,24 | 16,25 | 21,73 | 42,70 |
| Spark + cache + Parquet | 11,00 | 12,38 | 17,94 | 19,81 | 19,26 | 36,79 |

**57M (sentetik ×3) — işlem süresi (sn) ve bellek:**

| | pandas + CSV | pandas + Parquet | Spark + cache + CSV | Spark + cache + Parquet |
|---|---:|---:|---:|---:|
| Hazırlık | 56,81 | 1,41 | 5,62 + 25,86 | 8,16 + 17,41 |
| groupby | 2,02 | 2,15 | **0,76** | 0,96 |
| join | 5,71 | 8,48 | **0,71** | 0,93 |
| window | **6,21** | 9,36 | 9,76 | 9,32 |
| **Uçtan uca** | 70,8 | **21,4** | 42,7 | 36,8 |
| Tepe bellek (GB) | 7,9 | **9,1** | 6,9 | 6,8 |

**Dosya düzeni kontrolü** (19M Parquet, aynı satırlar; 3 → 23 row group):

| Adım | pandas: 3 rg | pandas: 23 rg | Spark: 3 rg | Spark: 23 rg |
|---|---:|---:|---:|---:|
| yükleme | 0,37 | 0,25 | — | — |
| groupby | 0,70 | 0,69 | 0,91 | 0,85 |
| join | 2,13 | 2,04 | 0,71 | 0,64 |
| window | 2,09 | 2,08 | 6,10 | **3,85** |

**Spark oturum açılışı:** `spark` motoru 11 bağımsız süreçte 5,62 ± 0,16 sn; `spark_cache` motoru 11 süreçte 6,81 ± 0,52 sn. Açılış kodu iki motorda birebir aynı; fark ölçüm oturumundan (makinenin o anki durumu) geliyor. Uçtan uca tabloda ölçülen değerler kullanıldı; bu, cache motorunu ~1,2 sn dezavantajlı gösterir.

**Kesişim noktaları** (log-log ara değer, `plots_processing.crossover_rows`):

| Karşılaştırma | Spark'ın pandas'ı geçtiği nokta |
|---|---|
| Uçtan uca, CSV (Spark ve Spark + cache) | ≈ 16M kayıt |
| Uçtan uca, Parquet | 57M'e kadar geçmiyor |
| join (Spark + Parquet, Spark + cache) | ≈ 4,4–4,7M kayıt |
| groupby (Spark + cache) | ≈ 16–19M kayıt |
| window (Spark + cache + Parquet) | ≈ 57M (başa baş) |

Grafikler: [Şekil 21](../reports/figures/21_s5_op_time.png) (işlem başına süre), [Şekil 22](../reports/figures/22_s5_end_to_end.png) (uçtan uca), [Şekil 23](../reports/figures/23_s5_memory.png) (tepe bellek), [Şekil 26](../reports/figures/26_s5_time_breakdown.png) (sürenin adımlara dökümü). Dashboard: [`reports/dashboard/index.html`](../reports/dashboard/index.html).

## Teknik yorumlar ("neden?")

- **Ölçeklenme eğimleri farklı.** 100K → 19M (190×) arasında pandas'ta süre işlemle doğrusal artıyor: groupby 100×, join 130×, window 230×. 19M → 57M (3×) arasında pandas 2,9–4,5×, Spark + cache 1,3–2,9× artıyor. Sebep: pandas tek çekirdekte vektörel çalışıyor, iş miktarı kayıtla doğrusal. Spark 20 iş parçacığına bölüyor ve küçük veride süresinin çoğu sabit maliyet (planlama, görev başlatma, sonucu sürücüye toplama: işlem başına ~0,25–0,5 sn; oturum açılışı ~5,6 sn).
- **Format, motordan daha belirleyici.** Spark tembel çalıştığı ve önbellek kullanılmadığı için CSV her işlemde yeniden parse ediliyor: 19M'de groupby CSV'de 5,9 sn, Parquet'te 0,9 sn (6,4×). Parquet sütunlu: yalnızca gereken kolonlar ikili ve sıkıştırılmış biçimde okunuyor. pandas'ta CSV maliyeti bir kez, yüklemede ödeniyor ama büyük: 19M CSV 18,8 sn (~1M kayıt/sn, tek çekirdek); aynı veri Parquet'ten 0,37 sn'de okunuyor (51×). 57M'de bu fark 56,8 sn vs 1,4 sn.
- **Önbellek (cache) Spark'ı pandas gibi çalıştırıyor, ama bedeli yükleme.** Önbellekteki tabloda Spark groupby ve join'de 19M'de bile pandas'tan hızlı (join 0,47 vs 1,97 sn; 57M'de 0,71 vs 5,71 sn). Önbelleğe alma ise pahalı: Parquet'te 8,9 sn (dosya 3 görevle okunup 20 bölüme dağıtılıyor), pandas'ta aynı yükleme 0,37 sn (pyarrow kolonları paralel çözüp doğrudan NumPy dizisine yazıyor). CSV'de ise Spark'ın paralel parse'ı pandas'ı geçiyor (10,5 vs 18,8 sn). Bu yüzden cache CSV'de ilk turda kâra geçiyor, Parquet'te ancak iş yükü ~3 kez tekrarlanınca (işlem başına kazanç 3,1 sn, yükleme maliyeti 8,9 sn).
- **join'de Spark kazanıyor.** pandas `merge` 19M satırlık birleşik tabloyu bellekte yeni bir kopya olarak oluşturuyor, sonra ayrı bir `groupby` çalıştırıyor. Spark küçük tabloyu tüm görevlere yayınlıyor (broadcast hash join). Birleştirmeyi ve kısmi toplamayı tek aşamada (whole-stage codegen) 20 iş parçacığında yapıyor; birleşik tabloyu hiç oluşturmuyor. Fark 57M'de 8–9×'e çıkıyor.
- **window'da pandas kazanıyor, ama farkın bir kısmı dosya düzeninden.** Spark + Parquet window'u 19M'de 6,1 sn; aynı veriyi 23 row group'la yazınca 3,85 sn. Yani 3 row group → 3 görevle okuma, Spark'ın window süresinin ~%37'sini oluşturuyor; pandas'a göre fark 4,0 sn'den 1,8 sn'ye iniyor (farkın yarısından fazlası dosya düzeninden). Kalanı window'un kendi maliyeti: `partitionBy(PULocationID)` tüm satırların bölgeye göre yeniden dağıtılmasını (shuffle: diske yaz + oku) ve her bölümde sıralamayı gerektiriyor; dağılım da dengesiz (en yoğun bölge verinin %4,7'si). pandas aynı işi tek vektörel sıralama ve `groupby.head(3)` ile, veri taşımadan yapıyor. Önbellekte ve 20 bölümde Spark window 19M'de 3,3 sn (pandas 2,1), 57M'de ise başa baş (9,3 vs 9,4 sn).
- **Bellek.** Spark'ın JVM tabanı (~0,5 GB) var; dosyadan okuyan Spark 19M'de 4–5 GB, önbellekli Spark 5,7–6,4 GB kullanıyor. Önbellekli Spark'ın tepe belleği 57M'de 6,8–6,9 GB'ta duruyor, veriyle büyümüyor: süreç belleğini 6 GB'lık sabit JVM heap'i sınırlıyor. Sığmayan önbellek bölümlerinin diske taşması (`MEMORY_AND_DISK` varsayılanı) olası mekanizma; bu koşuda Spark UI ile ayrıca doğrulanmadı. pandas'ın tepe belleği 6 kolonla ~0,14–0,16 KB/kayıt doğrusal artıyor; 57M ölçümü (9,1 GB) 19M'den gelen eğimi doğruladı. **57M'de ilk kez pandas Spark'tan fazla bellek kullanıyor.** Bu eğimle 32 GB RAM ~200M kayıtta dolar (tüm 20 kolonla, S1'in ~0,5 KB/kayıt eğimiyle ~65M).
- **pandas'ın da ölçeklenme sorunu var.** 19M → 57M'de pandas + Parquet join 4,0× ve window 4,5× yavaşladı (veri 3×); CSV'den yüklenen tabloda 2,9–3,0×. Parquet'ten yüklenen tablonun 57M'de neden daha yavaş işlendiği bu çalışmada ayrıştırılmadı (olası neden: `PULocationID`'nin Parquet'te int32, bölge tablosunda int64 olması ve merge'deki tip dönüşümü kopyası).

## Araştırma sorusuna cevap

> *Veri büyüdükçe klasik analiz yaklaşımı ne zaman yetersiz kalıyor?*

Bu makinede (32 GB RAM, 20 iş parçacığı) ve 57M kayda kadar:

1. **Klasik yaklaşım (pandas + CSV) ~16M kayıtta Spark'ın gerisine düşüyor.** Darboğaz işlem değil, tek çekirdekli CSV parse. 57M'de pandas + CSV 70,8 sn, Spark + cache + CSV 42,7 sn.
2. **Formatı değiştirmek motoru değiştirmekten daha etkili.** pandas + Parquet 57M'de bile uçtan uca en hızlı seçenek (21,4 sn; Spark'ın en iyisi 36,8 sn). Ama süresi veriden hızlı artıyor (19M → 57M: 3× veri, 4× süre); Spark + cache 1,9×. Eğilim sürerse Parquet'te de kesişim 57M'in ötesinde, birkaç yüz milyon kayıt civarında; bu ölçülmedi.
3. **Yetersizlik işleme göre değişiyor.** join'de Spark 4,5M'den itibaren önde (57M'de 8–9×). groupby'da önbellekli Spark ~16–19M'den sonra önde. window'da pandas 57M'e kadar önde ya da başa baş. "Büyük veri → Spark" genellemesi işlem türünü hesaba katmalı.
4. **Spark'ın verimi dosya düzenine ve çalışma modeline bağlı.** Tek dosya + büyük row group, Spark'ı 1–3 çekirdekte bırakıyor (window 6,1 → 3,85 sn). Önbellek, işlemleri hızlandırıyor ama yükleme maliyeti ancak tekrarlanan sorgularda amorti oluyor.
5. **Klasik yaklaşımın sert sınırı bellek.** 57M'de pandas 9,1 GB ile Spark'ı (6,8 GB) geçti ve doğrusal büyümeye devam ediyor; Spark'ın belleği sabit heap'le sınırlı kalıyor. 32 GB'ta pandas 6 kolonla ~200M, 20 kolonla ~65M kayıtta çalışamaz hâle gelir.

## Sınırlamalar

- 57M sentetik: gerçek veri dağılımını korur ama her satır 3 kez geçer (window'da eşitlikler, sıralamada daha düşük entropi). Dosyadan okuyan `spark` motoru 57M'de ölçülmedi (dosya düzeni onu 10 row group'la sınırlıyor; ana soru için pandas vs önbellekli Spark yeterli).
- Spark oturum açılışı iki motor arasında ~1,2 sn farklı ölçüldü (aynı kod, farklı ölçüm oturumu). Uçtan uca karşılaştırmada bu fark cache motorunun aleyhine.
- Ölçümler tek makinede ve `local[*]` modunda yapıldı. Kümede yatay ölçeklenme ve RAM'i tamamen aşan veri test edilmedi; bellek sınırları doğrusal ekstrapolasyondur (57M ölçümüyle doğrulanan eğim).
- Bütün ölçümler sıcak işletim sistemi önbelleğinde yapıldı. Soğuk diskten okuma CSV'nin dezavantajını artırır.
- Öğrenci 1'in okuma ölçümleri başka bir makinede (28 çekirdek, 16 GB) alındı; mutlak süreler doğrudan karşılaştırılamaz. Öğrenci 1'in Spark + Parquet okuma ölçümleri de aynı dosya düzeninden (1–4 row group) etkilenmiş olabilir.
- İşlemler aynı süreçte sabit sırayla çalıştı (groupby → join → window). JVM heap'i ve Python'un ayırdığı bellek işletim sistemine hemen geri verilmediği için işlem başına tepe bellek bir "yüksek su işareti"dir. Uçtan uca tepe bellek (tablolar ve Şekil 23) bundan etkilenmez.

## Yeniden üretim

```powershell
python -m src.build_sizes                                   # boyut setleri (5,3 GB), S1
python -m src.benchmark_processing                          # 5 boyut × 3 motor × 2 format (~15 dk), tutarlılık kontrolüyle
python -m src.scale_synthetic --factor 3                    # sentetik 57M (CSV 5,7 GB + Parquet 1,1 GB, ~3 dk)
python -m src.benchmark_processing --sizes x3 --engines pandas spark_cache    # ~12 dk
python -m src.scale_synthetic --layout                      # 19M Parquet, 16 MB row group
python -m src.benchmark_processing --sizes full_rg16 --formats parquet_snappy
python -m src.plots_processing                              # Şekil 21–23, 26
python -m src.dashboard                                     # reports/dashboard/index.html
python -m pytest tests/test_benchmark_processing.py tests/test_plots_processing.py tests/test_dashboard.py tests/test_scale_synthetic.py -v
```

Dizüstüde ölçümleri **performans modunda ve fişte** alın; dengeli modda süreler ~2,7× uzar.

[README §10](../README.md#10-öğrenci-5--işleme-performansı-ve-dashboard) · [Notebook](../notebooks/05_processing_benchmark.ipynb) · [Sınav soruları](exam_questions_s5.md)
