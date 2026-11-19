# Kısa teknik rapor — Öğrenci 3

Ocak–Mayıs 2026 dönemindeki 17.210.961 temiz Yellow Taxi yolculuğunu PySpark ile biniş saati, takvim tarihi, biniş bölgesi ve ödeme türüne göre özetledim. Her ana özetin grup sayımları toplandığında 17.210.961 kayıt elde edildi. Biniş saatleri New York yerel saatine göre okunur.

| Analiz | Yöntem ve sonuç |
|---|---|
| Saat | Yerel saate göre sayımda 18.00–18.59, 1.150.635 binişle ilk sırada. Hafta günü × saat sayımlarını ilgili günün dönemdeki görülme sayısına böldüm. Hafta içi zirvesi perşembe 21.00 (yaklaşık 8.830), hafta sonu zirvesi cumartesi 18.00 (yaklaşık 8.853). |
| Gün | 151 kesintisiz takvim gününü saydım. Tarih sıralı pencerede bugünü ve önceki altı günü ortaladım. `lag(7)` ile önceki haftanın aynı gününü aldım. 25 Nisan'daki 153.232 biniş, 18 Nisan'daki 141.356 binişten %8,4 fazla. Ayrı hesaplanan 19–25 Nisan hareketli ortalaması 123.804. |
| Bölge | Biniş koduna göre saydım ve TLC ad tablosunu sol birleştirmeyle ekledim. Upper East Side South 815.297 binişle ilk sırada. İlçe içinde bölgeleri sıralayıp biriken binişleri ilçe toplamına böldüm. En az %80'e ilk kez Queens'te 4/69, Manhattan'da 30/67 bölgeyle ulaşıldı. Harita 17.210.743 binişi kapsıyor. Sınırla eşleşmeyen 218 biniş genel toplamda duruyor. |
| Ödeme | Ödeme türü sayısını tüm grupları kapsayan pencerenin toplamına böldüm. Kredi kartının kayıt payı %64,96 ve ortalama `total_amount` değeri 29,05 USD. Temel ücreti pozitif ve kayıtlı bahşişi sıfır veya üzerinde olan 11.180.868 kart kaydında toplam kayıtlı bahşişi toplam `fare_amount` değerine böldüm. Ağırlıklı oran %23,50. |

Sonuçlar veri dönemindeki kayıtlı binişleri anlatır. Nakit bahşiş bu ölçümde yer almaz. Yoğunluk farklarının nedenini belirlemek için ek veri gerekir.

[README §8](../README.md#8-öğrenci-3--aggregation-analizleri) temiz veri ve bölge tablosunun hazırlanmasını açıklar. `python -m src.aggregate_trips --analysis all --export` özet CSV'lerini üretir. [Notebook](../notebooks/03_aggregation.ipynb) bu CSV'lerden grafikleri oluşturur. [Testler](../tests/test_aggregate_trips.py) gruplama, pencere ve eşik davranışlarını küçük veri örnekleriyle kontrol eder.
