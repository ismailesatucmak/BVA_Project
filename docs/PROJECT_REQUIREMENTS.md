# Proje Gereksinimleri (resmi metin — değiştirilmez)

> Hocanın verdiği metin olduğu gibi aktarıldı. Her karar bu dosyaya göre kontrol edilir.
> Karşılıkları: [ROADMAP.md §5–6](ROADMAP.md).

## Görevlerim

- **Öğrenci 1:** NYC Taxi verisini temin eder, veri sözlüğünü hazırlar, CSV/Parquet dönüşümünü ve veri büyüklüğü analizini yapar.
- **Öğrenci 2:** Eksik/hatalı kayıtları temizler; anormal mesafe, ücret ve süre değerlerini analiz eder.

## Proje Değerlendirme Kıstasları

| Ölçüt | Puan |
|---|---|
| Problemin Big Data niteliği ve mimari tasarım | 5 |
| Çalışan teknik uygulama | 7 |
| Veri işleme ve analitik derinlik | 5 |
| Deney, karşılaştırma ve nicel sonuç | 5 |
| 5 öğrencinin dengeli teknik katkısı | 5 |
| Sunum ve sorulara hâkimiyet | 3 |
| **Toplam** | **30** |

## Proje Hazırlanırken Dikkat Edilecekler

- Gerçek veya açık kaynaklı bir veri kümesi kullanılmalı. Veri yeterince büyük değilse sentetik olarak ölçeklendirilmeli.
- En az 3 farklı veri büyüklüğünde deney yapılmalı; örneğin 100 bin, 1 milyon, 5 milyon kayıt.
- Probleme uygun en az bir ölçeklenebilir veri işleme teknolojisi kullanılmalıdır; Spark/PySpark önerilen seçeneklerden biridir.
- Uygun projelerde en az iki veri formatı veya depolama yaklaşımı karşılaştırılmalıdır; örneğin CSV ve Parquet.
- aggregation, join, window, graph, vector search veya streaming işlemlerinden probleme uygun en az bir ileri analitik işlem.
- En az bir performans ölçümü yapılmalı: işlem süresi, bellek, throughput, latency, disk boyutu vb.
- En az bir görselleştirme bulunmalı.
- Sonuçta yalnızca "çalıştı" değil, neden böyle sonuç verdiği yorumlanmalı.
- Kod/notebook, veri kaynağı, mimari şema ve kısa teknik rapor teslim edilmeli (Git + README + requirements.txt/environment.yml).
- Her öğrencinin teknik katkısı ayrı gösterilmeli.
- Proje ile alakalı çoktan seçmeli 5 seçenekli 10 sınav sorusu hazırlanacaktır.
