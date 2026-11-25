# Kısa Teknik Rapor — Öğrenci 5

Öğrenci 1'in boyut setleri (100K / 1M / 5M / 10M / 19M kayıt; CSV ve Parquet snappy) üzerinde üç ileri analitik işlemin **işleme** süresini ve belleğini pandas ile PySpark arasında karşılaştırdım. Bütün sonuçları tek bir dashboard'da topladım. Okuma ve depolama performansı Öğrenci 1'in alanıdır ([`io_benchmark.csv`](../reports/results/io_benchmark.csv)); burada tekrar ölçülmedi.

## Yöntem

| Bileşen | Seçim ve gerekçe |
|---|---|
| **İşlemler** | **groupby:** biniş saatine göre yolculuk sayısı, ortalama mesafe, ortalama ücret ve toplam gelir (24 grup). **join:** yolculuklar ⋈ TLC bölge tablosu (265 satır), ilçeye göre sayım ve ortalama tutar. **window:** her biniş bölgesinde en yüksek tutarlı 3 yolculuk; `row_number() over (partition by PULocationID order by total_amount desc)`. |
| **Girdi** | Ham veri boyut setleri (`build_sizes`; xxhash64 ile iç içe örneklem). İki motor da aynı 6 kolonu okur. CSV'de tarih parse edilir; Spark CSV'ye Parquet şeması verilir. |
| **Çalışma modeli** | Her motor doğal yoluyla çalışır. **pandas:** veri bir kez belleğe yüklenir (`load`), işlemler bellekteki tabloda çalışır. **Spark:** tembel değerlendirme nedeniyle her işlem dosyadan okuyarak çalışır; sonuç küçük olduğu için `toPandas()` ile tetiklenir. Spark oturum açılışı (`startup`) ayrı ölçülür. |
| **Uçtan uca süre** | pandas = yükleme + 3 işlem. Spark = oturum açılışı + 3 işlem. |
| **Tekrar** | Her işlem 1 ısınma + 3 tekrar (ortalama ± std). İşletim sistemi önbelleği sıcak. Spark oturum açılışı her süreçte bir kez ölçüldü. |
| **İzolasyon** | Her motor × boyut × format ayrı alt süreçte çalıştı; pandas ve Spark aynı anda çalışmadı. |
| **Bellek** | Her adım boyunca süreç ağacının (Python + JVM) tepe RSS'i, `src/mem_probe.PeakRSS` ile 50 ms'de bir örneklendi. |
| **Doğruluk kontrolü** | Her işlemin sonucu satır sayısı + checksum ile kaydedildi. 5 boyut × 3 işlemde 2 motor × 2 formatın tamamı aynı sonucu verdi (`check_consistency`). |
| **Makine** | Intel i7-13700H (14 çekirdek / 20 iş parçacığı), 32 GB RAM, NVMe SSD, Windows 11. Spark `local[*]`, 6 GB sürücü belleği. |

## Sonuçlar

Kaynak: [`s5_processing.csv`](../reports/results/s5_processing.csv) (özet) · [`s5_processing_raw.csv`](../reports/results/s5_processing_raw.csv) (her tekrar).

**19M kayıt, işlem süresi (sn, 3 tekrar ortalaması):**

| | pandas + CSV | pandas + Parquet | Spark + CSV | Spark + Parquet |
|---|---:|---:|---:|---:|
| Hazırlık (pandas yükleme / Spark açılış) | 18,82 | 0,37 | 5,48 | 5,81 |
| groupby (saat) | 0,70 | 0,70 | 5,86 | 0,91 |
| join (ilçe) | 1,97 | 2,13 | 4,21 | **0,71** |
| window (bölge başına ilk 3) | 2,07 | 2,09 | 6,62 | 6,10 |
| **Uçtan uca** | 23,6 | **5,3** | 22,2 | 13,5 |
| Tepe bellek (GB) | 2,7 | 3,1 | 4,9 | 4,2 |

**Uçtan uca süre (sn) — boyuta göre:**

| | 100K | 1M | 5M | 10M | 19M |
|---|---:|---:|---:|---:|---:|
| pandas + CSV | 0,16 | 1,34 | 6,58 | 12,73 | 23,56 |
| pandas + Parquet | 0,05 | 0,27 | 1,31 | 2,61 | 5,28 |
| Spark + CSV | 6,81 | 9,33 | 11,96 | 15,11 | 22,17 |
| Spark + Parquet | 6,45 | 7,23 | 10,56 | 11,89 | 13,53 |

**Kesişim noktaları** (log-log ara değer, `plots_processing.crossover_rows`):
- Uçtan uca, **CSV**: Spark ≈ **16,1M kayıttan** sonra pandas'ı geçiyor.
- Uçtan uca, **Parquet**: 19M'e kadar Spark pandas'ı geçmiyor.
- Tek işlem olarak **join + Parquet**: Spark ≈ **4,5M kayıttan** sonra pandas'tan hızlı. 19M'de fark 3×.

Grafikler: [Şekil 21](../reports/figures/21_s5_op_time.png) (işlem başına süre), [Şekil 22](../reports/figures/22_s5_end_to_end.png) (uçtan uca), [Şekil 23](../reports/figures/23_s5_memory.png) (tepe bellek). Dashboard: [`reports/dashboard/index.html`](../reports/dashboard/index.html).

## Teknik yorumlar ("neden?")

- **Ölçeklenme eğimleri farklı.** Veri 190 kat büyüyor (100K → 19M). pandas'ta süre işlemle doğrusal artıyor: groupby 100×, join 130×, window 230×. Spark + Parquet'te groupby 3,8×, join 2,9× artıyor. Sebep şu: pandas tek çekirdekte vektörel çalışıyor, iş miktarı kayıtla doğrusal. Spark 20 iş parçacığına bölüyor ve küçük veride süresinin çoğu sabit maliyetten oluşuyor:
  - planlama
  - görev başlatma
  - sonucu sürücüye toplama
  
  Bu sabit maliyet işlem başına ~0,25 sn. Oturum açılışı ise ~5,5 sn sabit.
- **Format, motordan daha belirleyici.** Spark tembel çalıştığı ve önbellek kullanılmadığı için CSV her işlemde yeniden parse ediliyor. 19M'de groupby CSV'de 5,9 sn, Parquet'te 0,9 sn sürüyor (6,4×). Parquet sütunlu: yalnızca gereken kolonlar ikili ve sıkıştırılmış biçimde okunuyor, metin parse edilmiyor. pandas'ta CSV maliyeti yalnızca bir kez, yüklemede ödeniyor ama büyük: 19M CSV 18,8 sn (~1M kayıt/sn, tek çekirdek) sürüyor, aynı veri Parquet'ten 0,37 sn'de okunuyor (51×). Bu, Öğrenci 1'in okuma bulgusuyla tutarlı.
- **join'de Spark kazanıyor.** pandas `merge` 19M satırlık birleşik tabloyu bellekte yeni bir kopya olarak oluşturuyor, sonra ayrı bir `groupby` çalıştırıyor. Bu yüzden 19M Parquet'te tepe bellek 3,1 GB. Spark küçük tabloyu tüm görevlere yayınlıyor (broadcast hash join). Birleştirmeyi ve kısmi toplamayı tek aşamada (whole-stage codegen) 20 iş parçacığında yapıyor; birleşik tabloyu hiç oluşturmuyor.
- **window'da pandas kazanıyor.** `partitionBy(PULocationID)` tüm satırların bölgeye göre yeniden dağıtılmasını gerektiriyor. Bu shuffle'da 19M satır diske yazılıp yeniden okunuyor, ardından her bölümde sıralama yapılıyor. Dağılım da dengesiz: en yoğun bölge (237) verinin %4,7'sini tek bir göreve yüklüyor. pandas aynı işi tek bir vektörel sıralama ve `groupby.head(3)` ile, veri taşımadan yapıyor. Tek makinede shuffle'ın getirisi yok, yalnızca maliyeti var. Spark window ayrıca en fazla belleği kullanan adım (4,2–4,9 GB).
- **Bellek.** Spark'ın JVM tabanı (~0,5 GB) var ve shuffle tamponlarıyla 19M'de 4–5 GB'a çıkıyor. pandas'ın tepe belleği 6 kolonla kayıt başına ~0,14–0,16 KB artıyor (19M'de 2,7–3,1 GB). Bu eğimle 32 GB RAM ~200M kayıtta dolar. Öğrenci 1'in 20 kolonlu ölçümünde eğim ~0,5 KB/kayıttı; 16 GB makinede ~32M kayıt. Hangi kolonların okunduğu, sınırı birkaç kat kaydırıyor.

## Araştırma sorusuna cevap

> *Veri büyüdükçe klasik analiz yaklaşımı ne zaman yetersiz kalıyor?*

Bu makinede (32 GB RAM) ve 19M kayda kadar:

1. **Klasik yaklaşım (pandas + CSV) süre açısından ~16M kayıtta Spark'ın gerisine düşüyor.** Darboğaz işlem değil, tek çekirdekli CSV parse. Spark + Parquet ile kıyaslandığında bu nokta ~9M kayda iniyor (10M'de 12,7 sn vs 11,9 sn).
2. **Formatı değiştirmek motoru değiştirmekten daha etkili.** pandas + Parquet 19M'de bile en hızlı seçenek (uçtan uca 5,3 sn; Spark + Parquet 13,5 sn). Spark'ın sabit maliyetleri (oturum ~5,5 sn, işlem başına ~0,25 sn) tek makinede ancak çok büyük veride veya join gibi paralelleşen işlemlerde amorti oluyor.
3. **Yetersizlik işleme göre değişiyor.** join'de Spark 4,5M'den itibaren önde; window'da 19M'de hâlâ 3× geride. "Büyük veri → Spark" genellemesi işlem türünü hesaba katmalı.
4. **Klasik yaklaşımın sert sınırı bellek.** RAM dolduğunda pandas çalışamaz. Spark diske taşma (spill) ve dağıtım ile devam eder. Bu sınır okunan kolon sayısına bağlı: 32 GB'ta 6 kolonla ~200M, tüm 20 kolonla ~65M kayıt (S1'in ~0,5 KB/kayıt eğimiyle).

## Sınırlamalar

- Spark işlemleri dosyadan okuyarak ölçüldü (motorun doğal kullanımı). `df.cache()` ile tekrarlanan işlemlerde Spark daha hızlı olurdu; bu senaryo ölçülmedi.
- Ölçümler tek makinede ve `local[*]` modunda yapıldı. Kümede yatay ölçeklenme ve RAM'i aşan veri (gerçek spill) test edilmedi. Bellek sınırları doğrusal ekstrapolasyondur.
- Bütün ölçümler sıcak işletim sistemi önbelleğinde yapıldı. Soğuk diskten okuma CSV'nin dezavantajını artırır.
- Öğrenci 1'in okuma ölçümleri başka bir makinede (28 çekirdek, 16 GB) alındı. Mutlak süreler bu raporla doğrudan karşılaştırılamaz, eğilimler karşılaştırılabilir.
- İşlemler aynı süreçte sabit sırayla çalıştı (groupby → join → window). JVM heap'i ve Python'un ayırdığı bellek işletim sistemine hemen geri verilmediği için işlem başına tepe bellek bir "yüksek su işareti"dir; özellikle sondaki window adımının belleği öncekilerden etkilenebilir. Uçtan uca tepe bellek (tablo ve Şekil 23) bundan etkilenmez.
- Spark oturum açılışı her süreçte tek ölçüm (std yok). Beş boyutta 5,48–5,81 sn arasında tutarlı çıktı.

## Yeniden üretim

```powershell
python -m src.build_sizes              # boyut setleri (5,3 GB), S1
python -m src.benchmark_processing     # 20 kombinasyon (~10 dk), tutarlılık kontrolüyle
python -m src.plots_processing         # Şekil 21–23
python -m src.dashboard                # reports/dashboard/index.html
python -m pytest tests/test_benchmark_processing.py tests/test_plots_processing.py tests/test_dashboard.py -v
```

[README §10](../README.md#10-öğrenci-5--işleme-performansı-ve-dashboard) · [Notebook](../notebooks/05_processing_benchmark.ipynb) · [Sınav soruları](exam_questions_s5.md)
