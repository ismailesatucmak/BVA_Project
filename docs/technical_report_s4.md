# Kısa Teknik Rapor — Öğrenci 4

Ocak–Mayıs 2026 dönemindeki 17.210.961 temiz Yellow Taxi yolculuğu üzerinde PySpark MLlib ile yolculuk süresi tahmini (`trip_duration_min`, 1 < süre ≤ 180 dk) için ölçeklenebilir bir ML pipeline kurdum ve klasik `scikit-learn` yaklaşımıyla 100K, 1M ve 5M kayıtlık alt örneklemlerde hız, doğruluk ve bellek açısından karşılaştırdım.

## Yöntem

| Bileşen | Seçim ve gerekçe |
|---|---|
| **Hedef** | `trip_duration_min` — `src/quality.py` içindeki `add_derived()` tanımı; temiz veride 1 < süre ≤ 180 dk |
| **Feature seçimi** | `trip_distance`, `pickup_hour`, `pickup_dow`, `extra`, `mta_tax`, `tolls_amount`, `improvement_surcharge`, `cbd_congestion_fee` (sayısal); `PULocationID`, `DOLocationID` (kategorik) |
| **Dışlanan kolonlar** | `fare_amount`, `total_amount`, `tip_amount`, `tpep_dropoff_datetime` (data leakage); `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee` (Flex Fare kayıtlarında kasıtlı null, ~%25,5) |
| **Kategorik kodlama** | `StringIndexer(handleInvalid="keep")` + `OneHotEncoder` (bölge ID'leri için) ve `VectorAssembler` |
| **Modeller** | PySpark `LinearRegression` (baseline) vs `GBTRegressor(maxIter=15, maxDepth=5, seed=42)`; scikit-learn `Ridge` vs `HistGradientBoostingRegressor(max_iter=100, max_depth=5)` |
| **Ölçüm izolasyonu** | `scikit-learn` kıyaslaması ayrı `subprocess` içinde çalıştırıldı; her boyut için tepe RSS bellek (`psutil`) ölçüldü |

## Sonuçlar

Tüm deney sonuçları [`reports/results/s4_spark_results.csv`](../reports/results/s4_spark_results.csv) ve [`reports/results/s4_comparison.csv`](../reports/results/s4_comparison.csv) dosyalarında yer alır:

| Framework | Model | Boyut | RMSE (dk) | MAE (dk) | R² | Eğitim (sn) | Tahmin (sn) | Tepe Python RAM (MB) |
|---|---|---|---:|---:|---:|---:|---:|---:|
| PySpark | LinearRegression | 100K | 7,1924 | 4,9083 | 0,6467 | 4,04 | 0,50 | 72,1 |
| PySpark | GBTRegressor | 100K | 6,2407 | 4,1061 | 0,7340 | 10,74 | 0,34 | 72,3 |
| PySpark | LinearRegression | 1M | 7,1871 | 4,9370 | 0,6487 | 5,19 | 2,04 | 72,4 |
| PySpark | GBTRegressor | 1M | 6,2729 | 4,1399 | 0,7324 | 61,93 | 0,86 | 72,4 |
| PySpark | LinearRegression | 5M | 7,1933 | 4,9254 | 0,6459 | 18,10 | 7,84 | 72,3 |
| PySpark | GBTRegressor | 5M | 6,2842 | 4,1415 | 0,7297 | 1303,91 | 4,25 | 72,4 |
| scikit-learn | Ridge | 100K | 7,3340 | 5,1205 | 0,6260 | 0,01 | 0,0004 | 2441,7 |
| scikit-learn | HistGradientBoosting | 100K | 5,9556 | 3,9040 | 0,7534 | 0,25 | 0,0292 | 2443,3 |
| scikit-learn | Ridge | 1M | 7,4000 | 5,1281 | 0,6276 | 0,04 | 0,0042 | 2540,7 |
| scikit-learn | HistGradientBoosting | 1M | 5,9275 | 3,8755 | 0,7611 | 1,93 | 0,3199 | 2545,9 |
| scikit-learn | Ridge | 5M | 7,4119 | 5,1396 | 0,6261 | 0,23 | 0,0136 | 3124,6 |
| scikit-learn | HistGradientBoosting | 5M | 5,9263 | 3,8849 | 0,7610 | 12,16 | 1,5821 | 3132,2 |

**Özellik önem düzeyleri (`s4_feature_importance.csv`, GBTRegressor 1M):** `trip_distance` (**0,6201**) açık ara en belirleyici değişkendir; ardından `pickup_hour` (**0,1444**), `pickup_dow` (**0,1005**), `cbd_congestion_fee` (**0,0273**) ve bölge kodları (`DOLocationID` / `PULocationID`) gelir.

## Teknik Yorumlar

- **Doğrusal olmayan modeller belirgin biçimde üstün:** PySpark içinde `GBTRegressor` (RMSE 6,24–6,28 dk, R² ~0,73), `LinearRegression`'a (RMSE 7,19 dk, R² ~0,65) göre RMSE'de ~0,91 dk (~%13) iyileşme sağlar. Trafik hızı mesafeyle doğrusal ölçeklenmez; kısa şehir içi yolculuklar ile otoyol/havalimanı yolculukları ve yoğun saatler (`pickup_hour`, `pickup_dow`) ağaç tabanlı eşik bölmeleriyle yakalanır.
- **R² tavanı (~0,73–0,76):** 100K'dan 5M'e çıkıldığında metriklerin neredeyse sabit kalması (`R²` 0,734 → 0,730), model kapasitesinden değil özellik uzayındaki bilgi sınırından kaynaklanır. Anlık trafik sıkışıklığı, hava durumu ve yol çalışmaları gibi dışsal faktörler veride bulunmadığı için kalan ~%24–27 varyans açıklanamaz.
- **PySpark vs scikit-learn bellek ve hız dengesi:** `scikit-learn` tek makinede 5M kaydı da tamamlayabilmiş ve Cython tabanlı histogram algoritmasıyla 12,16 sn gibi çok kısa sürede eğitilmiştir (`HistGradientBoosting`, `max_iter=100` ile R²=0,7610; PySpark `GBTRegressor` ise görev kısıtı olan `maxIter=15` ve yüksek boyutlu one-hot vektör nedeniyle R²=0,7297 ve 1303,91 sn). Ancak **kritik darboğaz bellektir:** `scikit-learn` veriyi belleğe yüklemek zorunda olduğu için 100K örneklemde dahi 2,44 GB, 5M örneklemde **3,13 GB** tepe RAM tüketmiştir. PySpark'ta Python sürücüsü ~72 MB'ta sabit kalmış, 5M GBT sırasında JVM bellek sınırı dolduğunda (`MemoryStore` 3,0 GiB) veriyi otomatik olarak diske spill ederek çökmeden tamamlamıştır. Veri tek makine RAM'ini aştığında (ör. tam 17,2M+ kayıt ve one-hot matrisler) klasik yaklaşım bellek duvarına çarpar.

## Yeniden Üretim

```powershell
python -m src.fetch_data
python -m src.ml_duration
python -m pytest tests/test_ml_duration.py -v
```

[README §9](../README.md#9-öğrenci-4--ml-modeli) · [Notebook](../notebooks/04_ml_duration_prediction.ipynb) · [Testler](../tests/test_ml_duration.py) · Grafikler: [Şekil 18](../reports/figures/18_s4_scalability.png) · [Şekil 19](../reports/figures/19_s4_model_comparison.png) · [Şekil 20](../reports/figures/20_s4_feature_actual_pred.png)
