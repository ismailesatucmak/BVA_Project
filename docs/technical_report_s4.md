# Kısa Teknik Rapor — Öğrenci 4

Ocak–Mayıs 2026 dönemindeki 17.210.961 temiz Yellow Taxi yolculuğundan alınan 100K / 1M / 5M kayıtlık örneklemlerde yolculuk süresini (`trip_duration_min`, 1 < süre ≤ 180 dk) tahmin ettim. Aynı modelleri PySpark MLlib ve scikit-learn ile **aynı koşullarda** eğitip doğruluk, süre ve bellek açısından karşılaştırdım.

> **Revizyon notu.** İlk sürümdeki karşılaştırma üç ölçüm hatası içeriyordu. Bu sürümde düzeltildi ve tüm sayılar yeniden üretildi.
> 1. scikit-learn işçisi 17,2M satırın tamamını belleğe okuyup sonra örnekliyordu. Bu yüzden "100K'da bile 2,44 GB" sonucu modelin değil okumanın maliyetiydi.
> 2. Spark belleği yalnızca Python sürücüsünden ölçülüyordu (~72 MB); işi yapan JVM ölçülmüyordu. Ölçülen değer de tepe değil, eğitim sonrasındaki anlık değerdi.
> 3. scikit-learn modellerine bölge bilgisi verilmemişti ve iterasyon sayıları farklıydı (100 vs 15). Ayrıca özellik önemi tablosundaki `DOLocationID_40` gibi isimler bölge numarası değil, StringIndexer sıra numarasıydı.

## Yöntem

| Bileşen | Seçim ve gerekçe |
|---|---|
| **Hedef** | `trip_duration_min`. `src/quality.py` → `add_derived()` |
| **Örneklem** | S1'in xxhash64 anahtarı (`build_sizes.with_sample_key`) kullanıldı. Örneklem makineden bağımsız ve iç içe: 100K ⊂ 1M ⊂ 5M. Train/test ayrımı (%80/%20) da satır hash'iyle yapıldı ve örneklemle birlikte diske yazıldı. **İki çerçeve aynı satırlarda eğitilir ve test edilir.** |
| **Özellikler (iki tarafta aynı)** | 8 sayısal kolon: `trip_distance`, `pickup_hour`, `pickup_dow`, `extra`, `mta_tax`, `tolls_amount`, `improvement_surcharge`, `cbd_congestion_fee`. Bunlara ek olarak biniş ve iniş bölgesi. |
| **Bölge kodlaması** | Kodlar yalnızca eğitim kümesinden öğrenildi: en sık 254 bölge 0…253 kodunu aldı, kalanlar ve eğitimde görülmeyenler 254 ("diğer") koduna düştü. 255 kod sınırı, sklearn `HistGradientBoosting`'in kategorik özellik sınırından geliyor. |
| **Dışlanan kolonlar** | `fare_amount`, `total_amount`, `tip_amount`, `tpep_dropoff_datetime` (leakage). `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee` (Flex Fare'de kasıtlı null). `dropna()` kullanılmadı. |
| **Doğrusal model** | Spark `LinearRegression(regParam=0)` ve sklearn `LinearRegression`. İkisi de düzenlileştirmesiz OLS. Bölge kodu one-hot kodlandı ve son kategori düşürüldü; tasarım matrisi iki tarafta aynı. Bir birim test, iki çerçevenin aynı veride aynı tahmini verdiğini doğruluyor. |
| **Ağaç modeli** | Spark `GBTRegressor` ve sklearn `HistGradientBoostingRegressor`. Ortak ayarlar: 50 ağaç, derinlik 5, öğrenme oranı 0,1, yaprakta en az 20 kayıt, erken durdurma yok. Bölge iki tarafta da **kategorik** özellik (Spark: nominal metadata + `maxBins=256`). İlk sürümde Spark tarafında 500+ boyutlu one-hot vektör kullanılıyordu. |
| **Ölçüm** | Her çerçeve × model × boyut **ayrı alt süreçte** çalıştı. Veri yükleme süreye dahil değil. Bellek, eğitim ve tahmin boyunca süreç ağacının (Python + Spark JVM + Python işçileri) **tepe RSS**'i olarak ölçüldü. `src/mem_probe.PeakRSS` her 50 ms'de örnekliyor. |
| **Makine** | Intel i7-13700H (14 çekirdek / 20 iş parçacığı), 32 GB RAM, Windows 11, Spark `local[*]` + 6 GB sürücü belleği |

## Sonuçlar

Kaynak: [`reports/results/s4_comparison.csv`](../reports/results/s4_comparison.csv). Test kümesi her boyutta kaydın %20'si.

| Çerçeve | Model | Boyut | RMSE (dk) | MAE (dk) | R² | Eğitim (sn) | Tepe RSS (MB) |
|---|---|---|---:|---:|---:|---:|---:|
| PySpark | LinearRegression | 100K | 7,202 | 4,909 | 0,643 | 3,2 | 739 |
| scikit-learn | LinearRegression | 100K | 7,201 | 4,909 | 0,643 | 0,5 | 210 |
| PySpark | GBTRegressor | 100K | 6,000 | 3,830 | 0,752 | 22,6 | 1.131 |
| scikit-learn | HistGradientBoosting | 100K | 5,714 | 3,687 | 0,775 | 0,2 | 212 |
| PySpark | LinearRegression | 1M | 7,170 | 4,924 | 0,648 | 5,0 | 1.238 |
| scikit-learn | LinearRegression | 1M | 7,170 | 4,924 | 0,648 | 6,9 | 746 |
| PySpark | GBTRegressor | 1M | 5,548 | 3,587 | 0,789 | 52,5 | 2.803 |
| scikit-learn | HistGradientBoosting | 1M | 5,571 | 3,611 | 0,787 | 1,1 | 553 |
| PySpark | LinearRegression | 5M | 7,175 | 4,926 | 0,647 | 12,3 | 1.815 |
| scikit-learn | LinearRegression | 5M | 7,176 | 4,926 | 0,647 | 32,1 | 3.071 |
| PySpark | GBTRegressor | 5M | **5,481** | 3,554 | **0,794** | 197,4 | 4.161 |
| scikit-learn | HistGradientBoosting | 5M | 5,561 | 3,604 | 0,788 | 4,8 | 2.209 |

**Özellik önemi** (Spark GBT, 1M, [`s4_feature_importance.csv`](../reports/results/s4_feature_importance.csv)):

| Özellik | `trip_distance` | `DOLocationID` | `pickup_hour` | `PULocationID` | `pickup_dow` | diğer 5 ücret kalemi |
|---|---:|---:|---:|---:|---:|---:|
| Önem | 0,466 | 0,184 | 0,135 | 0,125 | 0,071 | toplam 0,019 |

## Teknik yorumlar ("neden?")

- **Karşılaştırma artık adil.** OLS iki çerçevede aynı sonucu verdi (RMSE farkı ≤ 0,001 dk). Doğruluk farkları veri veya özellik farkından gelmiyor; yalnızca algoritma uygulamasından geliyor.
- **Doğrusal olmayan model ~%23 daha iyi.** GBT, LinearRegression'a göre RMSE'yi 7,17'den 5,48 dakikaya indiriyor (R² 0,65 → 0,79). Süre mesafeyle doğrusal artmıyor. Bölge ve saat etkileşimleri (Manhattan içi yoğun saat, havalimanı otoyolu) ağaç bölmeleriyle yakalanıyor. Bölgenin toplam önemi (0,31), saat ve günün toplamından (0,21) büyük.
- **Veri arttıkça yalnızca esnek model kazanıyor.** LinearRegression'ın R²'si 100K'dan 5M'e sabit kalıyor (0,643–0,648). Doğrusal model mesafe × bölge × saat etkileşimlerini temsil edemediği için hatası yanlılıktan (bias) gelir; daha fazla veri bu hatayı azaltmaz. GBT ise 0,752 → 0,794'e çıkıyor: 2 × 254 kategorili bölge özelliğinde nadir bölgeler ancak büyük örneklemde "yaprakta en az 20 kayıt" koşulunu sağlayıp kendi bölmesini alabiliyor.
- **Tek makinede scikit-learn çok daha hızlı.** GBT'de fark 100K'da ~110×, 5M'de ~40×. Bunun dört nedeni var:
  - Spark'ın her ağaç seviyesi ayrı bir iş (job) olarak zamanlanıyor.
  - Veri JVM ile Python arasında serileştiriliyor.
  - Görev başına sabit bir ek yük var.
  - Histogram toplamaları shuffle üzerinden birleştiriliyor.
  sklearn ise önceden binlenmiş `uint8` matris üzerinde çok iş parçacıklı C++/Cython kodu çalıştırıyor. Spark'ın göreli dezavantajı veri büyüdükçe azalıyor (110× → 40×), çünkü sabit ek yük toplam sürenin daha küçük bir kısmı oluyor.
- **Doğrusal modelde sıra 1M'de tersine dönüyor.** sklearn LinearRegression 1M'de 6,9 sn ve 5M'de 32,1 sn sürüyor; Spark 5,0 sn ve 12,3 sn. Spark normal denklemleri (XᵀX, 516 × 516) tek geçişte ve paralel kuruyor. sklearn ise seyrek matriste iteratif `lsqr` çözücüsünü tek çekirdekte çalıştırıyor. Yani algoritma ve paralellik seçimi motor seçiminden daha belirleyici.
- **Bellek: Spark'ın sabit bir taban maliyeti var.** Boş bir JVM ve önbellek ~0,5 GB tutuyor. Spark GBT 5M'de 4,2 GB'a çıkıyor (önbellekteki veri, ağaç başına histogram tamponları, JVM heap'i). sklearn HistGradientBoosting 2,2 GB'ta kalıyor.
- **Klasik yaklaşımın da sınırı var.** sklearn LinearRegression 5M'de Spark'tan fazla bellek kullanıyor (3,1 GB vs 1,8 GB), çünkü seyrek one-hot matris ve çözücünün kopyaları birlikte bellekte tutuluyor. sklearn belleği kayıt sayısıyla doğrusal artıyor: ~0,6 KB/kayıt (LinearRegression) ve ~0,4 KB/kayıt (HGB). Bu makinede 32 GB RAM, doğrusal ekstrapolasyonla ~55–80M kayıtta dolar. Spark bu noktadan sonra da diske taşma (spill) ve küme üzerinde dağıtma ile çalışabilir. Ancak bunu bu projede ölçmedik.
- **Cevap (S4 açısından).** 5M kayıta kadar, tek makinede ve RAM'e sığan veride klasik yaklaşım hem daha hızlı hem daha az bellek kullanıyor. Doğrulukta fark küçük: 5M'de Spark GBT RMSE'si 0,08 dk daha iyi. Spark'ın getirisi veri RAM'i aştığında veya birden çok makineye dağıtmak gerektiğinde ortaya çıkar. "Veri büyüdü, Spark'a geçelim" kararı ölçeğe ve algoritmaya göre verilmeli.

## Sınırlamalar

- `tolls_amount` ve `cbd_congestion_fee` rota bilgisi taşıyor ve yolculuk sonunda kesinleşiyor; biniş anında bilinmiyorlar. Önemleri küçük (toplam 0,011), ama gerçek zamanlı bir modelde çıkarılmaları gerekir.
- İki GBT uygulaması birebir aynı değil. Spark ilk ağaca ortalama değerle başlamıyor; sklearn başlıyor. Sayısal kolonların bin sınırları da farklı yöntemlerle hesaplanıyor. Bu yüzden ağaç modellerinde 0,02–0,08 dk'lık RMSE farkı uygulamadan geliyor.
- Ölçümler tek makinede ve `local[*]` modunda yapıldı. Spark'ın küme üzerinde yatay ölçeklenmesi test edilmedi. Sonuçlar ilk sürümün makinesiyle (28 çekirdek, 16 GB) doğrudan karşılaştırılamaz.
- Hiperparametre araması yapılmadı. Amaç en iyi modeli bulmak değil, iki çerçeveyi aynı ayarlarla karşılaştırmaktı.

## Yeniden üretim

```powershell
python -m src.fetch_data                 # temiz veri
python -m src.ml_duration                # örneklemler + 12 ölçüm (~10 dk) + Şekil 18–20
python -m src.ml_duration --plots-only   # yalnızca grafikler (Spark gerekmez)
python -m pytest tests/test_ml_duration.py -v
```

[README §9](../README.md#9-öğrenci-4--ml-modeli-yolculuk-süresi-tahmini) · [Notebook](../notebooks/04_ml_duration_prediction.ipynb) · [Testler](../tests/test_ml_duration.py) · Grafikler: [Şekil 18](../reports/figures/18_s4_scalability.png) · [Şekil 19](../reports/figures/19_s4_model_comparison.png) · [Şekil 20](../reports/figures/20_s4_feature_actual_pred.png)
