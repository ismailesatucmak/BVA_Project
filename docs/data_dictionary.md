# Veri Sözlüğü — NYC Yellow Taxi 2026 (Ocak–Mayıs)

> **Kaynaklar:** TLC resmi veri sözlüğü (`data/raw/data_dictionary_trip_records_yellow.pdf`, 18 Mart 2025) +
> gerçek veriden Spark ile hesaplanan istatistikler (`python -m src.dictionary_stats` →
> `reports/results/dictionary_stats.csv`, `code_value_counts.csv`, `flex_fare_check.csv`).
> **Toplam:** 5 dosya, **18.999.282 kayıt**, 20 kolon. Her satır = taksimetre ile kaydedilmiş bir yolculuk.

## 1. Genel Notlar

- **Veriyi TLC üretmez.** Taksimetre sağlayıcıları (TPEP/TSP) toplar ve TLC'ye iletir; TLC doğruluğunu garanti etmez (User Guide, s.3). Hatalı kayıtların kaynağı budur → Öğrenci 2'nin temizlik gerekçesi.
- **Zaman damgaları saat dilimi içermez** (`timestamp_ntz`): New York yerel saati olarak yorumlanır.
- **Para birimi:** USD. **Mesafe:** mil (taksimetrenin ölçtüğü).
- **Kolon adı farkı:** veride `Airport_fee` (büyük A), resmi sözlükte `airport_fee`. Kodda verideki ad kullanılır.
- **Negatif tutarlar** tüm ücret kolonlarında görülür (iade / iptal / düzeltme kayıtları) → temizlikte ele alınır.

## 2. Kolonlar

"Gözlenen" sütunları 19M kaydın tamamından hesaplandı. "Beklenen" sütunu resmi tanım ve TLC ücret tarifesine dayanır.

### 2.1 Kimlik, zaman ve konum

| Kolon | Tip | Açıklama | Birim | Beklenen / izinli | Gözlenen min – max | Null % | Örnek |
|---|---|---|---|---|---|---|---|
| `VendorID` | int | Kaydı sağlayan taksimetre şirketi | kod | 1, 2, 6, 7 (bkz. §3.1) | 1 – 7 | 0 | 2 |
| `tpep_pickup_datetime` | timestamp_ntz | Taksimetrenin açıldığı an | tarih-saat | 2026-01-01 … 2026-05-31 | **2001-01-01** – 2026-06-01 | 0 | 2026-01-01 00:54:04 |
| `tpep_dropoff_datetime` | timestamp_ntz | Taksimetrenin kapandığı an | tarih-saat | ≥ pickup | 2001-01-01 – 2026-06-02 | 0 | 2026-01-01 00:59:37 |
| `PULocationID` | int | Biniş bölgesi (TLC Taxi Zone) | kod | 1–263 (264/265 = bilinmiyor) | 1 – 265 | 0 | 239 |
| `DOLocationID` | int | İniş bölgesi (TLC Taxi Zone) | kod | 1–263 (264/265 = bilinmiyor) | 1 – 265 | 0 | 238 |
| `store_and_fwd_flag` | string | Kayıt bağlantı yokken araç hafızasında tutulup sonra mı gönderildi? | Y/N | Y, N | N, Y | **25,33** | N |

### 2.2 Yolculuk

| Kolon | Tip | Açıklama | Birim | Beklenen / izinli | Gözlenen min – max | Null % | Örnek |
|---|---|---|---|---|---|---|---|
| `passenger_count` | bigint | Yolcu sayısı (sürücü girer) | kişi | 1–6 | 0 – 9 | **25,33** | 1 |
| `trip_distance` | double | Taksimetrenin ölçtüğü mesafe | mil | > 0 | 0.0 – **328.522,2** | 0 | 0.97 |
| `RatecodeID` | bigint | Yolculuk sonunda geçerli tarife | kod | 1–6, 99 (bkz. §3.2) | 1 – 99 | **25,33** | 1 |
| `payment_type` | bigint | Ödeme yöntemi | kod | 0–6 (bkz. §3.3) | 0 – 4 | 0 | 1 |

### 2.3 Ücret bileşenleri

`total_amount` ≈ `fare_amount` + `extra` + `mta_tax` + `tip_amount` + `tolls_amount` + `improvement_surcharge` + `congestion_surcharge` + `Airport_fee` + `cbd_congestion_fee`

| Kolon | Tip | Açıklama | Birim | Beklenen | Gözlenen min – max | Null % | Örnek |
|---|---|---|---|---|---|---|---|
| `fare_amount` | double | Taksimetrenin süre + mesafeye göre hesapladığı ücret | USD | > 0 | −2.555,20 – 5.525,99 | 0 | 7.20 |
| `extra` | double | Çeşitli ek ücretler (gece, yoğun saat vb.) | USD | ≥ 0 | −7,50 – 20,71 | 0 | 1.00 |
| `mta_tax` | double | Tarifeye göre otomatik eklenen MTA vergisi | USD | 0 veya 0,50 | −0,50 – 10,50 | 0 | 0.50 |
| `tip_amount` | double | Bahşiş — **yalnızca kredi kartı**; nakit bahşiş yok | USD | ≥ 0 | −222 – 766 | 0 | 3.66 |
| `tolls_amount` | double | Ödenen köprü/tünel geçiş ücretleri toplamı | USD | ≥ 0 | −111,79 – 1.400 | 0 | 0.00 |
| `improvement_surcharge` | double | Binişte alınan iyileştirme ek ücreti (2015'ten beri) | USD | 0 veya 1,00 | −1,00 – 2,50 | 0 | 1.00 |
| `total_amount` | double | Yolcudan alınan toplam (nakit bahşiş hariç) | USD | > 0 | −2.560,20 – 5.530,74 | 0 | 15.86 |
| `congestion_surcharge` | double | New York eyaleti tıkanıklık ek ücreti | USD | 0 veya 2,50 | −2,50 – 2,50 | **25,33** | 2.50 |
| `Airport_fee` | double | Yalnızca LaGuardia / JFK'den binişte | USD | 0 veya 1,75 | −2,00 – 27,00 | **25,33** | 0.00 |
| `cbd_congestion_fee` | double | MTA Congestion Relief Zone ücreti (5 Ocak 2025'ten beri) | USD | 0 veya 0,75 | −0,75 – 0,75 | 0 | 0.00 |

## 3. Kod Tabloları (gözlenen dağılım, 19M kayıt)

### 3.1 `VendorID`
| Kod | Anlamı | Kayıt | % |
|---|---|---|---|
| 1 | Creative Mobile Technologies, LLC | 3.679.227 | 19,37 |
| 2 | Curb Mobility, LLC | 15.048.722 | 79,21 |
| 6 | Myle Technologies Inc | 36.577 | 0,19 |
| 7 | Helix | 234.756 | 1,24 |

### 3.2 `RatecodeID`
| Kod | Anlamı | Kayıt | % |
|---|---|---|---|
| 1 | Standart tarife | 12.857.761 | 67,68 |
| 2 | JFK | 433.958 | 2,28 |
| 3 | Newark | 57.991 | 0,31 |
| 4 | Nassau veya Westchester | 40.405 | 0,21 |
| 5 | Pazarlıklı ücret | 158.985 | 0,84 |
| 6 | Grup yolculuğu | 13 | 0,00 |
| 99 | Null / bilinmiyor | 637.889 | 3,36 |
| *null* | Flex Fare (bkz. §4) | 4.812.280 | 25,33 |

### 3.3 `payment_type`
| Kod | Anlamı | Kayıt | % |
|---|---|---|---|
| 0 | Flex Fare | 4.812.280 | 25,33 |
| 1 | Kredi kartı | 12.273.572 | 64,60 |
| 2 | Nakit | 1.675.104 | 8,82 |
| 3 | Ücretsiz | 65.643 | 0,35 |
| 4 | İtirazlı | 172.683 | 0,91 |
| 5 | Bilinmiyor | 0 | — |
| 6 | İptal edilmiş yolculuk | 0 | — |

### 3.4 `store_and_fwd_flag`
| Kod | Anlamı | Kayıt | % |
|---|---|---|---|
| N | Anlık gönderildi | 14.171.108 | 74,59 |
| Y | Hafızada tutulup sonra gönderildi | 15.894 | 0,08 |
| *null* | Flex Fare | 4.812.280 | 25,33 |

## 4. Flex Fare ve Eksik Değerler (önemli)

Veride null içeren **yalnızca 5 kolon** var: `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`.

`flex_fare_check.csv` sonucu:

| `payment_type = 0` mı? | Kayıt | 5 kolonun hepsi null | En az biri null |
|---|---|---|---|
| Evet | 4.812.280 | 4.812.280 (%100) | 4.812.280 |
| Hayır | 14.187.002 | 0 | 0 |

**Sonuç:** Eksik değerler rastgele değil, **tamamen Flex Fare kayıtlarına özgü**. Bunlar önceden fiyatı belirlenmiş (uygulama üzerinden) yolculuklar; taksimetre bu alanları doldurmuyor. Bu kayıtlar **silinmez**, `is_flex_fare` kolonuyla işaretlenir. "Null içeren satırı sil" türünden naif bir temizlik verinin **%25,3'ünü** (4,8M kayıt) yok ederdi.

## 5. Taxi Zone Lookup (`data/raw/taxi_zone_lookup.csv`)

| Kolon | Açıklama |
|---|---|
| `LocationID` | 1–265; `PULocationID` / `DOLocationID` ile eşleşir (join anahtarı) |
| `Borough` | İlçe: Queens 69, Manhattan 69, Brooklyn 61, Bronx 43, Staten Island 20, EWR (Newark) 1, Unknown 1, boş 1 |
| `Zone` | Mahalle düzeyinde bölge adı (NYC Planlama Dairesi'nin NTA'larına yakın) |
| `service_zone` | Boro Zone 205, Yellow Zone 55, Airports 2, EWR 1, boş 2 |

- 265 satır. **264 = "Unknown"**, **265 = "Outside of NYC"** → konum bilinmiyor (R8).
- Bölge sınırları (`data/raw/taxi_zones/*.shp`) harita görselleştirmesi için kullanılabilir.

## 6. Gözlenen Veri Kalitesi Sorunları (Öğrenci 2'ye devir)

| Sorun | Kanıt | İlgili kural |
|---|---|---|
| Kapsam dışı tarih | pickup min **2001-01-01**, max 2026-06-01 | R6 |
| Aşırı mesafe | `trip_distance` max **328.522 mil** | R2, R5 |
| Sıfır mesafe | min 0.0 | R2 |
| Negatif ücretler | `fare_amount` min −2.555, `total_amount` min −2.560 | R3 |
| Aşırı yolcu sayısı | `passenger_count` 0 ve 7–9 | R1 |
| Tanımsız tarife | `RatecodeID = 99` → 637.889 kayıt (%3,4) | R7 |
| Bilinmeyen bölge | `LocationID` 264/265 mevcut | R8 |
| Beklenenden yüksek sabit ücret | `mta_tax` max 10,50; `improvement_surcharge` max 2,50; `Airport_fee` max 27 | T2.3 anomali |

Dağılım detayları (yüzdelikler, kural etkileri) T2.1 profilinde hesaplanır.

## 7. Türetilecek Kolonlar (temiz veride eklenecek)

| Kolon | Tanım | Kullanan |
|---|---|---|
| `trip_duration_min` | (dropoff − pickup) dakika | S2 (R4), S3, S4 |
| `avg_speed_mph` | `trip_distance` / (süre saat) | S2 (R5), S4 |
| `pickup_hour` | pickup saati (0–23) | S3, S4 |
| `pickup_dow` | pickup haftanın günü (1=Pazar … 7=Cumartesi, Spark `dayofweek`) | S3, S4 |
| `is_flex_fare` | `payment_type = 0` | S2, S3, S4 |
| `pickup_year`, `pickup_month` | partition kolonları | `data/clean/` |

## 8. Clean Schema (Öğrenci 3/4/5 için arayüz)

**Konum:** `data/clean/` — Parquet (snappy), `pickup_year=…/pickup_month=…` ile partition'lı, ay başına 1 dosya, yolculuk zamanına göre sıralı. **17.210.961 kayıt, 479 MB.**
**Üretim:** `python -m src.clean` · Kurallar ve etkileri: [`cleaning_rules.md`](cleaning_rules.md).

```python
from src.spark_session import get_spark
spark = get_spark()
df = spark.read.parquet("data/clean")                      # tümü
jan = df.filter("pickup_month = 1")                        # partition pruning: yalnızca Ocak klasörü okunur
```

| Kolon | Tip | Not |
|---|---|---|
| 20 ham kolon | §2'deki gibi | değerler değiştirilmedi; yalnızca kurala takılan satırlar silindi |
| `trip_duration_min` | double | (dropoff − pickup) dakika; temiz veride 1 < süre ≤ 180 |
| `avg_speed_mph` | double | mesafe / süre (saat); temiz veride ≤ 80 |
| `pickup_hour` | int | 0–23 |
| `pickup_dow` | int | 1 = Pazar … 7 = Cumartesi (Spark `dayofweek`) |
| `is_flex_fare` | boolean | `payment_type = 0`; **True ise 5 kolon null** (§4) — analizlerde ayrı ele alın |
| `pickup_year`, `pickup_month` | int | partition kolonları (klasör adından gelir) |

**Temiz veride garanti edilenler:** ay aralığı 2026-01..05; mesafe 0 < x ≤ 100; ücret ve toplam > 0; yolcu sayısı null veya 1–6; `RatecodeID` ≠ 99; bölge ID'leri 1–263; tam kopya yok.
**Garanti edilmeyenler:** istatistiksel uç değerler (örn. 1.061 $ ücret) — bunlar silinmedi, T2.3'te analiz edilir.
