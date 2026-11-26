# Kısa Teknik Rapor — Öğrenci 4

Ocak–Mayıs 2026 dönemindeki 17.210.961 temiz Yellow Taxi yolculuğundan alınan 100K / 1M / 5M kayıtlık örneklemlerde ve **verinin tamamında (17M)** yolculuk süresini (`trip_duration_min`, 1 < süre ≤ 180 dk) tahmin ettim. Aynı modelleri PySpark MLlib ve scikit-learn ile **aynı koşullarda** eğitip doğruluk, süre ve bellek açısından karşılaştırdım. Ardından modelin kendisini üç ek analizle inceledim: özellik sızıntısı, hiperparametre araması ve hata analizi.

> **Revizyon notu.** Önceki sürümlerdeki karşılaştırma dört ölçüm hatası içeriyordu. Bu sürümde düzeltildi ve etkilenen tüm sayılar yeniden üretildi.
> 1. scikit-learn işçisi 17,2M satırın tamamını belleğe okuyup sonra örnekliyordu. Bu yüzden "100K'da bile 2,44 GB" sonucu modelin değil okumanın maliyetiydi.
> 2. Spark belleği yalnızca Python sürücüsünden ölçülüyordu (~72 MB); işi yapan JVM ölçülmüyordu. Ölçülen değer de tepe değil, eğitim sonrasındaki anlık değerdi.
> 3. scikit-learn modellerine bölge bilgisi verilmemişti ve iterasyon sayıları farklıydı (100 vs 15). Ayrıca özellik önemi tablosundaki `DOLocationID_40` gibi isimler bölge numarası değil, StringIndexer sıra numarasıydı.
> 4. **Spark 20 çekirdeğin yalnızca 1–2'sini kullanıyordu.** Örneklemler tek Parquet dosyası olarak (128 MB row group) yazılıyor: 100K–5M dosyası 1, 17M dosyası 2 row group içeriyor. Spark bir row group'u bölemediği için veriyi 1–2 bölümle okuyup bütün eğitimi 1–2 görevde çalıştırıyordu (Spark UI: tek aktif görev, ~2,4 çekirdek CPU). Artık veri, eğitimden önce çekirdek sayısı kadar bölüme dağıtılıyor (`spark_session.spread_partitions`, süreye dahil değil). Eski ve yeni sonuçlar aşağıda yan yana; fark tek başına bir bulgu.

## Yöntem

| Bileşen | Seçim ve gerekçe |
|---|---|
| **Hedef** | `trip_duration_min`. `src/quality.py` → `add_derived()` |
| **Örneklem** | S1'in xxhash64 anahtarı (`build_sizes.with_sample_key`) kullanıldı. Örneklem makineden bağımsız ve iç içe: 100K ⊂ 1M ⊂ 5M ⊂ 17M (17M = temiz verinin tamamı). Train/test ayrımı (%80/%20) da satır hash'iyle yapıldı ve örneklemle birlikte diske yazıldı. **İki çerçeve aynı satırlarda eğitilir ve test edilir.** |
| **Özellikler (iki tarafta aynı)** | 8 sayısal kolon: `trip_distance`, `pickup_hour`, `pickup_dow`, `extra`, `mta_tax`, `tolls_amount`, `improvement_surcharge`, `cbd_congestion_fee`. Bunlara ek olarak biniş ve iniş bölgesi. |
| **Bölge kodlaması** | Kodlar yalnızca eğitim kümesinden öğrenildi: en sık 254 bölge 0…253 kodunu aldı, kalanlar ve eğitimde görülmeyenler 254 ("diğer") koduna düştü. 255 kod sınırı, sklearn `HistGradientBoosting`'in kategorik özellik sınırından geliyor. |
| **Dışlanan kolonlar** | `fare_amount`, `total_amount`, `tip_amount`, `tpep_dropoff_datetime` (leakage). `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee` (Flex Fare'de kasıtlı null). `dropna()` kullanılmadı. |
| **Doğrusal model** | Spark `LinearRegression(regParam=0)` ve sklearn `LinearRegression`. İkisi de düzenlileştirmesiz OLS. Bölge kodu one-hot kodlandı ve son kategori düşürüldü; tasarım matrisi iki tarafta aynı. Bir birim test, iki çerçevenin aynı veride aynı tahmini verdiğini doğruluyor. |
| **Ağaç modeli** | Spark `GBTRegressor` ve sklearn `HistGradientBoostingRegressor`. Ortak ayarlar: 50 ağaç, derinlik 5, öğrenme oranı 0,1, yaprakta en az 20 kayıt, erken durdurma yok. Bölge iki tarafta da **kategorik** özellik (Spark: nominal metadata + `maxBins=256`). |
| **Paralellik** | sklearn: tüm çekirdekler (OpenMP). Spark: `local[*]` = 20 iş parçacığı; veri eğitimden önce 20 bölüme dağıtılır. |
| **Ölçüm** | Her çerçeve × model × boyut **ayrı alt süreçte** çalıştı. Veri yükleme ve dağıtma süreye dahil değil. Bellek, eğitim ve tahmin boyunca süreç ağacının (Python + Spark JVM + Python işçileri) **tepe RSS**'i olarak ölçüldü. `src/mem_probe.PeakRSS` her 50 ms'de örnekliyor. |
| **Makine** | Intel i7-13700H (14 çekirdek / 20 iş parçacığı), 32 GB RAM, Windows 11, Spark 6 GB sürücü belleği. Dizüstü fişte ve **performans modunda**. Dengeli güç modunda aynı ölçümler ~2,7× yavaş çıktı (sklearn HGB 5M: 13,2 sn vs 4,8 sn). Bu yüzden her ölçüm grubundan önce aynı kontrol ölçümü tekrarlandı: 4,88 ve 5,07 sn (ana ölçümdeki değer 4,84 sn). |

## Sonuçlar

Kaynak: [`reports/results/s4_comparison.csv`](../reports/results/s4_comparison.csv). Test kümesi her boyutta kaydın %20'si (17M'de 3,44M kayıt).

| Çerçeve | Model | Boyut | RMSE (dk) | MAE (dk) | R² | Eğitim (sn) | Tepe RSS (MB) |
|---|---|---|---:|---:|---:|---:|---:|
| PySpark | LinearRegression | 100K | 7,202 | 4,909 | 0,643 | 4,1 | 959 |
| scikit-learn | LinearRegression | 100K | 7,201 | 4,909 | 0,643 | 0,5 | 210 |
| PySpark | GBTRegressor | 100K | 5,963 | 3,804 | 0,755 | 103,4 | 2.484 |
| scikit-learn | HistGradientBoosting | 100K | 5,714 | 3,687 | 0,775 | 0,2 | 212 |
| PySpark | LinearRegression | 1M | 7,170 | 4,924 | 0,648 | 4,5 | 2.124 |
| scikit-learn | LinearRegression | 1M | 7,170 | 4,924 | 0,648 | 6,9 | 746 |
| PySpark | GBTRegressor | 1M | 5,538 | 3,577 | 0,790 | 104,7 | 4.615 |
| scikit-learn | HistGradientBoosting | 1M | 5,571 | 3,611 | 0,787 | 1,1 | 553 |
| PySpark | LinearRegression | 5M | 7,175 | 4,926 | 0,647 | 6,0 | 3.363 |
| scikit-learn | LinearRegression | 5M | 7,176 | 4,926 | 0,647 | 32,1 | 3.071 |
| PySpark | GBTRegressor | 5M | 5,469 | 3,551 | 0,795 | 140,7 | 6.159 |
| scikit-learn | HistGradientBoosting | 5M | 5,561 | 3,604 | 0,788 | 4,8 | 2.209 |
| PySpark | LinearRegression | 17M | 7,163 | 4,920 | 0,649 | **9,9** | 5.651 |
| scikit-learn | LinearRegression | 17M | 7,164 | 4,920 | 0,649 | 106,9 | **10.088** |
| PySpark | GBTRegressor | 17M | **5,453** | 3,549 | **0,796** | 269,1 | 6.918 |
| scikit-learn | HistGradientBoosting | 17M | 5,540 | 3,593 | 0,790 | **19,9** | 7.133 |

**Spark: bölüm sayısının etkisi** (eğitim süresi, sn). "1–2 bölüm" önceki sürümün ölçümü (100K–5M: commit `93ffe1d`; 17M: aynı gün, performans modu). RMSE iki durumda da aynı (≤ 0,04 dk fark).

| Model | Boyut | 1–2 bölüm (dosya düzeni) | 20 bölüm | Değişim |
|---|---|---:|---:|---:|
| LinearRegression | 100K | 3,2 | 4,1 | 1,3× yavaş |
| LinearRegression | 1M | 5,0 | 4,5 | 1,1× hızlı |
| LinearRegression | 5M | 12,3 | 6,0 | 2,1× hızlı |
| LinearRegression | 17M | 34,2 | 9,9 | 3,5× hızlı |
| GBTRegressor | 100K | 22,6 | 103,4 | **4,6× yavaş** |
| GBTRegressor | 1M | 52,5 | 104,7 | 2,0× yavaş |
| GBTRegressor | 5M | 197,4 | 140,7 | 1,4× hızlı |
| GBTRegressor | 17M | 990,5 | 269,1 | **3,7× hızlı** |

**Özellik önemi** (Spark GBT, 1M, [`s4_feature_importance.csv`](../reports/results/s4_feature_importance.csv)):

| Özellik | `trip_distance` | `DOLocationID` | `pickup_hour` | `PULocationID` | `pickup_dow` | diğer 5 ücret kalemi |
|---|---:|---:|---:|---:|---:|---:|
| Önem | 0,469 | 0,181 | 0,132 | 0,129 | 0,071 | toplam 0,019 |

### Ek analizler (`src/ml_analysis.py`, scikit-learn HistGradientBoosting)

Spark GBT bu makinede 13–500× yavaş olduğu için bu analizler aynı algoritma ailesinin sklearn uygulamasıyla yapıldı; ana deneyde iki uygulamanın RMSE farkı 1M ve üzerinde ≤ 0,1 dk.

**1. Özellik sızıntısı** (5M, ana deneydeki ayarlar, [`s4_ablation.csv`](../reports/results/s4_ablation.csv)):

| Özellik seti | Özellikler | RMSE (dk) | MAE (dk) | R² |
|---|---|---:|---:|---:|
| Tam | 8 sayısal + 2 bölge | 5,561 | 3,604 | 0,788 |
| **Biniş anı** | mesafe, saat, gün + 2 bölge (ücret kalemleri yok) | **5,536** | 3,571 | 0,790 |
| Yalnız bölge + zaman | saat, gün + 2 bölge (mesafe de yok) | 6,912 | 4,912 | 0,673 |

**2. Hiperparametre araması** ([`s4_tuning.csv`](../reports/results/s4_tuning.csv)). 12 ayarlık ızgara: ağaç sayısı {50, 200} × derinlik {5, 8, sınırsız (63 yaprak)} × öğrenme oranı {0,1, 0,2}. Seçim **yalnızca doğrulama kümesine** göre yapıldı: 1M eğitim kümesinin %20'si, satır hash'iyle ayrıldı. Test kümesi seçimde hiç kullanılmadı; seçilen ayar 5M eğitim kümesiyle yeniden eğitilip bir kez test edildi.

| Model (biniş anı özellikleri, 5M test) | Ayar | RMSE (dk) | MAE (dk) | R² |
|---|---|---:|---:|---:|
| Ana deneydeki ayar | 50 ağaç · derinlik 5 · lr 0,1 | 5,536 | 3,571 | 0,790 |
| **Arama ile seçilen** | 200 ağaç · sınırsız derinlik (63 yaprak) · lr 0,1 | **5,042** | **3,201** | **0,826** |

Doğrulamada en iyi iki ayar (200 ağaç, derinlik 8 ve sınırsız) 5,130 dk ile berabere; sıralama farkı 0,001 dk'nın altında.

**3. Hata analizi** (seçilen model, 5M test kümesi, 1,0M yolculuk, [`s4_error_analysis.csv`](../reports/results/s4_error_analysis.csv), [Şekil 25](../reports/figures/25_s4_error_analysis.png)):

| Boyut | Bulgu |
|---|---|
| Mesafe | MAE 0–1 milde 2,0 dk → 20+ milde 8,6 dk. Göreli hata ise düşüyor: MAE / ortalama süre %29 → %16. Kısa yolculukta birkaç dakikalık trafik ışığı/bekleme, süreye oranla büyük. |
| Saat | MAE 02–05 arasında 2,1–2,3 dk, 15:00'te en yüksek (4,1 dk). Öğleden sonra trafik yoğunluğu değişken; aynı mesafe ve bölgede süre daha dağınık. |
| İlçe | Manhattan çıkışlı yolculuklar (testin %88'i) MAE 2,9 dk; Queens 5,7 dk (havalimanı yolculukları uzun ve değişken). Staten Island (89) ve EWR (5) yolculuk sayısı az, değerleri güvenilir değil. |
| Sapma | Ortalama sapma (tahmin − gerçek) tüm mesafe, saat ve büyük ilçe gruplarında \|0,35\| dk'nın altında: model sistematik olarak fazla/az tahmin etmiyor; hata varyanstan geliyor. |

## Teknik yorumlar ("neden?")

- **Karşılaştırma adil.** OLS iki çerçevede aynı sonucu verdi (dört boyutta RMSE farkı ≤ 0,002 dk). Doğruluk farkları veri veya özellik farkından gelmiyor; yalnızca algoritma uygulamasından geliyor.
- **Doğrusal olmayan model ~%24 daha iyi.** GBT, LinearRegression'a göre RMSE'yi 7,16'dan 5,45 dakikaya indiriyor (R² 0,65 → 0,80). Süre mesafeyle doğrusal artmıyor. Bölge ve saat etkileşimleri (Manhattan içi yoğun saat, havalimanı otoyolu) ağaç bölmeleriyle yakalanıyor. Bölgenin toplam önemi (0,31), saat ve günün toplamından (0,20) büyük.
- **Veri arttıkça yalnızca esnek model kazanıyor, o da sınırlı.** LinearRegression'ın R²'si 100K'dan 17M'e sabit (0,643–0,649): doğrusal model mesafe × bölge × saat etkileşimlerini temsil edemediği için hatası yanlılıktan (bias) gelir; daha fazla veri bunu azaltmaz. GBT 0,755 → 0,795'e çıkıyor: nadir bölgeler ancak büyük örneklemde "yaprakta en az 20 kayıt" koşulunu sağlayıp kendi bölmesini alabiliyor. 5M → 17M'de kazanç 0,001: 50 ağaç × derinlik 5'lik model kapasitesinin sınırına geliniyor. Modeli büyütmek (arama: 200 ağaç, daha derin ağaç) 5M'de bile R²'yi 0,826'ya çıkarıyor; yani bu noktada **veri eklemek değil, modeli büyütmek** kazandırıyor.
- **Spark'ta paralellik veri boyutuna göre kazandırıyor ya da kaybettiriyor.** 20 bölümle GBT'nin süresi 100K'da ve 1M'de ~104 sn'de sabit: bu, veriden bağımsız bir koordinasyon maliyeti. 50 ağacın her seviyesi ayrı bir iş (job); her iş 20 görev başlatıyor, görevlerin histogramları sürücüde birleştiriliyor. Tek bölümde aynı iş tek görevle yapıldığı için küçük veride daha hızlı (22,6 sn). Veri büyüdükçe asıl hesap koordinasyon maliyetini geçiyor: 5M'de 20 bölüm 1,4×, 17M'de 3,7× hızlı. **Doğru bölüm sayısı veri boyutuna bağlı**; varsayılan "tek dosya Parquet" düzeni büyük veride Spark'ı tek çekirdeğe mahkûm ediyor.
- **Ağaç modelinde tek makinede scikit-learn hâlâ çok daha hızlı, ama fark kapanıyor.** sklearn GBT 100K'da ~500×, 1M'de ~90×, 5M'de ~29×, 17M'de ~14× hızlı. Spark'ın ~100 sn'lik sabit maliyeti büyük veride payını kaybediyor. Ancak marjinal maliyet de farklı: 5M → 17M arası her ek milyon kayıt Spark'ta ~10,5 sn, sklearn'de ~1,2 sn. sklearn önceden binlenmiş `uint8` matris üzerinde çok iş parçacıklı C++/Cython kodu çalıştırıyor; Spark her seviyede veriyi tarayıp histogramları görevler arasında birleştiriyor. Bu eğimlerle tek makinede Spark GBT hiçbir boyutta yetişemiyor; avantajı ancak veri tek makineye sığmadığında ortaya çıkar.
- **Doğrusal modelde Spark 1M'den itibaren önde, 17M'de ~11×.** sklearn LinearRegression 1M'de 6,9, 5M'de 32,1, 17M'de 106,9 sn; Spark 4,5 / 6,0 / 9,9 sn. Spark normal denklemleri (XᵀX, 516 × 516) tek geçişte ve 20 görevde paralel kuruyor. sklearn ise seyrek matriste iteratif `lsqr` çözücüsünü tek çekirdekte çalıştırıyor. Yani algoritma ve paralellik seçimi motor seçiminden daha belirleyici.
- **Bellek.** Spark'ın JVM tabanı (veri yüklü, eğitim başlamadan ~0,6–4 GB, boyutla artıyor: önbellekteki eğitim/test tabloları) var. 20 bölümle tepe bellek arttı (17M GBT 6,9 GB, LinearRegression 5,7 GB), çünkü 20 görev aynı anda kendi tamponunu tutuyor. sklearn HistGradientBoosting 17M'de 7,1 GB, yani Spark'la aynı düzeyde.
- **Klasik yaklaşımın sınırı: bellek-yoğun çözücü.** sklearn LinearRegression 17M'de **10,1 GB** kullanıyor (Spark 5,7 GB) ve 11× yavaş. Nedeni: seyrek one-hot matris ve çözücünün kopyaları birlikte bellekte tutuluyor. sklearn belleği kayıt sayısıyla doğrusal artıyor; 17M ölçümü 5M'den gelen eğimi doğruladı: ~0,58 KB/kayıt (LinearRegression) ve ~0,40 KB/kayıt (HGB). Bu makinede 32 GB RAM ~55M (LinearRegression) ve ~80M (HGB) kayıtta dolar. Spark ise belleği yetmediğinde diske taşarak (spill) devam edebiliyor.
- **Sızıntı endişesi doğrulukta bedel ödetmiyor.** `tolls_amount` ve `cbd_congestion_fee` çıkarılınca RMSE 5,561'den 5,536'ya iniyor (gürültü düzeyinde iyileşme). Bu iki kolonun taşıdığı bilgi (köprü/tünel, Manhattan güneyi) iniş bölgesinde zaten var. Mesafe ise vazgeçilmez: çıkarıldığında RMSE 6,91'e çıkıyor (R² 0,79 → 0,67). Bölge çifti mesafenin büyük kısmını taşısa da aynı bölge çifti içindeki farklı güzergâhları ayırt edemiyor.

## Araştırma sorusuna cevap (S4 açısından)

> *Veri büyüdükçe klasik analiz yaklaşımı ne zaman yetersiz kalıyor?*

1. **Cevap algoritmaya bağlı.** Doğrusal modelde klasik yaklaşım **1M kayıttan itibaren** Spark'ın gerisinde, 17M'de 11× yavaş ve 1,8× fazla bellek (10,1 GB). Ağaç modelinde (histogram tabanlı GBT) ise 17M'de bile 14× hızlı ve bellekte eşit.
2. **Sert sınır bellek.** Doğrusal eğimle bu makinede ~55M (doğrusal model) / ~80M (GBT) kayıtta RAM dolar. Bu nokta klasik yaklaşımın gerçekten çalışamadığı yerdir.
3. **Spark'ın verimi dosya düzenine bağlı.** Tek dosya Parquet + büyük row group, Spark'ı 1–2 çekirdekte bırakıyor (17M GBT: 990 sn → dağıtınca 269 sn). Spark'ı kullanmak, veriyi bölümlemeyi de bilmeyi gerektiriyor.
4. **Doğruluk için veri değil model önemli.** 5M → 17M R²'ye 0,001 ekledi; hiperparametre araması 5M'de 0,036 ekledi. Bu problemde "daha çok veri için büyük veri aracı" gerekçesi zayıf; kapasite artırımı tek makinede ucuz.

## Sınırlamalar

- `trip_distance` taksimetreden gelir, yani yolculuk sonunda kesinleşir. "Biniş anı" modelinde, navigasyon uygulamasının biniş anında hesapladığı planlanan rota mesafesinin yerine geçtiği varsayıldı. Gerçek planlanan mesafe veride yok.
- İki GBT uygulaması birebir aynı değil. Spark ilk ağaca ortalama değerle başlamıyor; sklearn başlıyor. Sayısal kolonların bin sınırları da farklı yöntemlerle hesaplanıyor. Bu yüzden ağaç modellerinde 0,03–0,25 dk'lık RMSE farkı uygulamadan geliyor.
- Ölçümler tek makinede ve `local[*]` modunda yapıldı. Spark'ın küme üzerinde yatay ölçeklenmesi test edilmedi. Bölüm sayısı yalnızca iki değerde (1–2 ve 20) denendi; boyuta göre en iyi değer aranmadı.
- Spark sürücü belleği 6 GB'ta sabit tutuldu (tüm boyutlarda aynı ayar).
- Her ölçüm tek tekrar: ML eğitimi uzun sürdüğü için 3 tekrar pratik değildi. Kontrol ölçümünde aynı iş 4,84 / 4,88 / 5,07 sn verdi (±%5); raporlanan farklar bunun çok üstünde.
- Hiperparametre araması yalnızca sklearn ile ve 12 ayarlık küçük bir ızgarada yapıldı. Ana karşılaştırma bilerek ortak (aranmamış) ayarlarla yapıldı; amaç en iyi modeli değil iki çerçeveyi aynı koşulda kıyaslamaktı.

## Yeniden üretim

```powershell
python -m src.fetch_data                 # temiz veri
python -m src.ml_duration                # örneklemler + 16 ölçüm (~20 dk) + Şekil 18–20
python -m src.ml_duration --sizes 17M --frameworks PySpark   # seçili satırlar (tablo birleştirilir)
python -m src.ml_duration --plots-only   # yalnızca grafikler (Spark gerekmez)
python -m src.ml_analysis                # sızıntı + arama + hata analizi (~2 dk) + Şekil 24–25
python -m pytest tests/test_ml_duration.py tests/test_ml_analysis.py -v
```

Dizüstüde ölçümleri **performans modunda ve fişte** alın; dengeli modda süreler ~2,7× uzar.

[README §9](../README.md#9-öğrenci-4--ml-modeli-yolculuk-süresi-tahmini) · [Notebook](../notebooks/04_ml_duration_prediction.ipynb) · [Testler](../tests/test_ml_duration.py) · Grafikler: [Şekil 18](../reports/figures/18_s4_scalability.png) · [Şekil 19](../reports/figures/19_s4_model_comparison.png) · [Şekil 20](../reports/figures/20_s4_feature_actual_pred.png) · [Şekil 24](../reports/figures/24_s4_ablation_tuning.png) · [Şekil 25](../reports/figures/25_s4_error_analysis.png)
