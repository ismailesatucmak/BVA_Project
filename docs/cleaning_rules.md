# Temizlik Kuralları ve Etkileri (Öğrenci 2 — T2.2)

> **Kod:** kontroller `src/quality.py`, kurallar `src/clean.py`, eşikler `src/config.py`.
> **Çalıştırma:** `python -m src.profile_data` (profil) → `python -m src.clean` (temizlik + `data/clean/`).
> **Sonuç dosyaları:** `reports/results/profile_raw.csv`, `logic_checks_raw.csv`, `cleaning_impact.csv`, `cleaning_before_after.csv`.

## 1. İlke

- **Bir kurala takılan kayıt silinir** (proje kararı). Kural listesi aşağıda; eşikler profile (T2.1) bakılarak doğrulandı.
- **İstisna — Flex Fare null'ları:** 5 kolonun (`passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`) null olması tek başına silme nedeni **değildir**. Bu null'lar yalnızca ve tam olarak `payment_type = 0` (Flex Fare, ön fiyatlı yolculuk) kayıtlarında görülür (%25,3) — eksik veri değil, iş kuralının sonucudur. Kontrollerde null değer hiçbir zaman "hatalı" sayılmaz.
- Kayıtlar `is_flex_fare` kolonuyla işaretlenir; sonraki analizler bu grubu ayrı ele alabilir.

## 2. Eksik değer stratejisi (kolon bazında)

| Kolon | Null oranı | Karar | Gerekçe |
|---|---|---|---|
| `passenger_count` | %25,3 | **Null bırakılır** | Flex Fare'de sürücü girmiyor; doldurmak (örn. 1) veri uydurmak olur ve yolcu analizini bozar |
| `RatecodeID` | %25,3 | Null bırakılır | Ön fiyatlı yolculukta tarife kodu anlamsız |
| `store_and_fwd_flag` | %25,3 | Null bırakılır | Teknik bayrak; analizde kullanılmıyor |
| `congestion_surcharge` | %25,3 | Null bırakılır | Ücret ön fiyata dahil; ayrıştırılamaz |
| `Airport_fee` | %25,3 | Null bırakılır | Aynı gerekçe |
| `avg_speed_mph` (türetilmiş) | süre ≤ 0 iken | Null | Sıfıra bölme; bu kayıtlar R4 ile zaten silinir |

Hiçbir kolonda **doldurma (imputation) yapılmadı**: eksiklik rastgele değil, sistematik (tek bir ödeme türüne bağlı). Ortalama/mod ile doldurmak Flex Fare yolculuklarını yanlışlıkla "standart" gösterirdi.

## 3. Kurallar ve etkileri (18.999.282 ham kayıt)

Üç sayı:
- **Tek başına:** kuralın yakaladığı kayıt (diğer kurallardan bağımsız).
- **Sıralı:** R1 → R10 sırasıyla uygulanınca bu kuralın sildiği (önceki kuralların bıraktıklarından).
- **Yalnız bu kural:** başka hiçbir kuralın yakalamadığı — kural kaldırılsa veride kalacak olan.

| # | Kural | Gerekçe | Tek başına | Sıralı | Yalnız bu kural |
|---|---|---|---|---|---|
| R1 | `passenger_count` 0 veya > 6 | fiziksel olarak geçersiz | 64.885 (%0,34) | 64.885 | 60.293 |
| R2 | `trip_distance` ≤ 0 veya > 100 mil | ölçüm hatası / iptal; p99,9 = 29,9 mil | 577.511 (%3,04) | 574.749 | 435.051 |
| R3 | `fare_amount` ≤ 0 veya `total_amount` ≤ 0 | iade, ters kayıt, ücretsiz | 129.194 (%0,68) | 116.605 | 39.603 |
| R4 | süre ≤ 1 dk, > 180 dk veya negatif | taksimetre hatası / kapatılmamış yolculuk; p99,9 = 125 dk | 436.497 (%2,30) | 291.597 | 285.399 |
| R5 | ortalama hız > 80 mph | fiziksel olarak imkânsız; p99,9 = 46,7 mph | 4.928 (%0,03) | 2.430 | 1.709 |
| R6 | pickup 2026-01-01 … 2026-05-31 dışında | hatalı zaman damgası (2001 tarihli kayıtlar dahil) | 15 | 8 | 8 |
| R7 | `RatecodeID` = 99 | sözlükte "Null/unknown" | 637.889 (%3,36) | 632.999 | 632.500 |
| R8 | `PULocationID` / `DOLocationID` ∈ {264, 265} | bilinmeyen bölge | 119.887 (%0,63) | 105.048 | 105.048 |
| R9 | tam kopya (ilki tutulur) — **window** `row_number()` | mükerrer kayıt | **0** | 0 | 0 |
| R10 | `cbd_congestion_fee` < 0 | 2025'te gelen kolonun tutarlılığı | 76.131 (%0,40) | **0** | 0 |
| **Toplam** | en az bir kurala takılan | | | **1.788.321 (%9,41)** | |

**Sonuç:** 18.999.282 → **17.210.961 kayıt (%90,6 korundu).**

### Kural etkisinden çıkan bulgular
- **Kurallar örtüşüyor.** Tek başına sayıların toplamı (2,09M) sıralı toplamdan (1,79M) büyük; aynı kayıt birden çok kurala takılıyor. Bu yüzden "kural başına silinen" yalnız tek sayıyla raporlanamaz.
- **R10 gereksiz çıktı:** negatif CBD ücretli 76.131 kaydın %100'ünün toplam tutarı da negatif → hepsini R3 zaten siliyor. Kural, gelecekte farklı veri gelirse diye tutuldu.
- **R9 sıfır:** 19M kayıtta tam kopya yok. Kural, tekrar üretilebilirlik ve gelecek veriler için tutuldu.
- **R7 ve R2 en büyük iki kural** (%3,3 ve %3,0) — aşağıdaki tartışmaya bakın.

### Naif temizlikle karşılaştırma
"Null içeren her satırı sil" (`dropna()`) **4.812.280 kayıt (%25,3)** silerdi — yani tüm Flex Fare yolculuklarını. Kural bazlı yaklaşım bunun yaklaşık üçte biri kadar siler ve sildiği her kaydın gerekçesi vardır.

## 4. Temizlik öncesi / sonrası

| Metrik | Ham | Temiz | Yorum |
|---|---|---|---|
| Kayıt | 18.999.282 | 17.210.961 | %9,4 silindi |
| Mesafe ortalama / medyan (mil) | 5,74 / 1,83 | 3,25 / 1,84 | **Ortalama %43 düştü, medyan değişmedi** → ortalamayı birkaç uç değer (328.522 mil) çarpıtıyordu |
| Mesafe max | 328.522 | 94,8 | |
| Ücret ortalama / medyan ($) | 21,27 / 15,60 | 20,51 / 15,19 | negatif iadeler ve uç değerler gitti |
| Ücret max ($) | 5.525,99 | 1.061,72 | hâlâ yüksek → T2.3 anomali analizi |
| Süre ortalama / medyan (dk) | 17,83 / 13,82 | 16,61 / 13,60 | 9.984 dk'lık (7 gün) kayıtlar gitti |
| Hız ortalama / medyan (mph) | 20,47 / 9,01 | 10,67 / 9,15 | ham ortalamayı 4,8 milyon mph'lik kayıtlar şişiriyordu |

**Ders:** Medyanlar neredeyse hiç değişmedi; ortalamalar ciddi değişti. Temizlenmemiş veride ortalamaya dayanan her analiz (örn. "ortalama yolculuk 5,7 mil") yanıltıcıdır.

## 5. Tartışma ve sınırlamalar

Profil sırasında iki büyük grubun incelenmesi, bunların **büyük olasılıkla gerçek yolculuklar** olduğunu gösterdi. Proje kararı gereği silindiler; sonraki analizler bu seçimin etkisini bilmeli:

| Grup | Kayıt | Kanıt |
|---|---|---|
| `RatecodeID = 99` (R7) | 637.889 | %99,9'u Vendor 1'den → kodu bu sağlayıcı göndermiyor. Medyan 7,8 mil, 45 dk, 32,5 $, %98'i farklı bölgeye, neredeyse tamamı kredi kartı → normal görünüyor. Silinmeleri **uzun yolculukların ve Vendor 1'in payını azaltır.** |
| Flex Fare + sıfır mesafe (R2'nin parçası) | 407.165 | Medyan 17 dk, 25,21 $, %95'i farklı bölgeye → yolculuk yapılmış, ama ön fiyatlı olduğu için taksimetre mesafe yazmamış. Silinmeleri **Flex Fare payını azaltır** (%25,3 → temiz veride daha düşük). |

| **Vendor 7 (Helix) — tamamı** (R4) | 234.756 | Bu sağlayıcının **her kaydında** iniş zamanı = biniş zamanı (süre 0), ama mesafe (medyan 1,5 mil) ve ücret (medyan 12 $) normal → sağlayıcı iniş zamanını göndermiyor. Temiz veride **Helix hiç yok** (ham verinin %1,2'si). Hata kayıtta değil, **veri kaynağında**. |

T2.3'teki kural × sağlayıcı kırılımı (`anomaly_rule_rates.csv`) bu tür kaynak kaynaklı örüntüleri gösterir: R7'nin %99,9'u Vendor 1'den (Bronx'ta kayıtların %54'ü, Brooklyn'de %30'u), R3'ün en yüksek oranı Vendor 6'da (%13,9).

Buna karşılık **normal (Flex olmayan) sıfır mesafeli** 169.579 kayıt gerçekten hatalı/iptal: medyan süre **11 saniye**, %75'i aynı bölgede başlayıp bitiyor. **Negatif ücretli** 118.331 kaydın hepsi Vendor 2'den ve %89'unun aynı zaman, bölge ve tutarda pozitif bir "eşi" var → orijinal yolculuğun iade/ters kaydı.

Diğer notlar:
- Zaman damgaları saat dilimsiz; 8 Mart 2026'daki yaz saati geçişini kapsayan yolculukların süresi ±60 dk sapabilir (çok az kayıt).
- Temiz veride hâlâ kural dışı kalan uç değerler var (ücret 1.061 $, `Airport_fee` 27 $) → istatistiksel anomali analizi T2.3'te.

## 6. Teknik not — küçük dosya sorunu

R9'un window işlemi veriyi Spark'ın varsayılan 200 shuffle partition'ına dağıtır. İlk yazımda bu, **ay başına 200 küçük dosya (toplam 1.000 dosya, 603 MB)** üretti. Ay başına tek partition'a toplanıp yolculuk zamanına göre sıralanınca **5 dosya, 479 MB** oldu: daha az dosya açma maliyeti, sıralı veri sayesinde daha iyi sıkıştırma ve zaman aralığı sorgularında row group atlama.
