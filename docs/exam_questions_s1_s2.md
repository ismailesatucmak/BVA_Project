# Sınav Soruları — Öğrenci 1 ve Öğrenci 2

Projenin 10 sorusundan Öğrenci 1 ve 2'nin payı: **4 ana soru** (her öğrenciye 2) + **2 yedek** (takım birleştirirken seçmek için). Hepsi 5 seçenekli, tek doğru cevaplı. Cevap anahtarı ve açıklamalar en altta.

---

## Ana sorular

**Soru 1 (Öğrenci 1 — veri formatı).** Projede aynı 19 milyon kayıtlık NYC Taxi verisi CSV olarak 1.907 MB, Parquet (zstd) olarak 290 MB yer kaplamıştır. Parquet'in bu kadar küçük olmasının **temel nedeni** aşağıdakilerden hangisidir?

- A) Parquet, null değer içeren satırları yazmadan atlar.
- B) Parquet verileri kolon bazında saklar; aynı kolonun benzer değerleri yan yana durduğu için dictionary/RLE encoding ve ikili (binary) tipler kullanılabilir, ardından sıkıştırılır.
- C) Parquet, veriyi otomatik olarak örnekleyip yalnızca bir kısmını saklar.
- D) Parquet, ondalık sayıları yuvarlayarak kayıplı (lossy) sıkıştırma yapar.
- E) CSV dosyaları her satırı iki kez (veri + yedek) sakladığı için büyüktür.

---

**Soru 2 (Öğrenci 1 — performans ölçümü).** Spark ile bir Parquet dosyasının okuma süresini ölçmek için `df.count()` süresine bakmak neden **yanıltıcıdır**?

- A) `count()` yalnızca ilk 1.000 satırı okuyup sonucu tahmin eder.
- B) `count()` Spark'ta tembel (lazy) olduğu için hiç çalışmaz.
- C) `count()` veriyi önce CSV'ye çevirdiği için ek süre ekler.
- D) Parquet dosyası satır sayısını kendi metadata'sında (footer) tuttuğu için `count()` kolon verilerini hiç çözmeden cevap verebilir; bu yüzden Parquet olduğundan çok daha hızlı görünür.
- E) `count()` yalnızca pandas'ta çalışır, Spark'ta hata verir.

---

**Soru 3 (Öğrenci 2 — eksik veri).** Veride 5 kolon (`passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`) kayıtların %25,3'ünde boştur ve bu boşluklar **yalnızca ve tam olarak** `payment_type = 0` (Flex Fare, ön fiyatlı yolculuk) olan kayıtlarda görülmektedir. Bu durumda en doğru yaklaşım hangisidir?

- A) `dropna()` ile null içeren tüm satırları silmek; eksik veri her zaman hatadır.
- B) Boş değerleri her kolonun ortalamasıyla doldurmak (mean imputation).
- C) Boşluğun rastgele değil bir iş kuralının sonucu olduğunu kabul etmek; null'u tek başına silme nedeni saymamak, kayıtları `is_flex_fare` gibi bir kolonla işaretleyip analizlerde ayrı ele almak.
- D) Boş değerleri 0 ile doldurmak; böylece toplam ücret hesapları bozulmaz.
- E) `payment_type` kolonunu silmek; çünkü boşluklara o neden oluyor.

---

**Soru 4 (Öğrenci 2 — anomali analizi).** Temiz veride global ücret eşiği (en yüksek %0,1 → 143 $ üstü) kullanılmaktadır. Aynı bölge içinde 0,5 millik bir yolculuk için 60 $ ücret alınmıştır; bu güzergâhın medyan ücreti 8 $'dır. Bu kaydı hangi yöntem **yakalar** ve neden?

- A) Global p99,9 eşiği yakalar, çünkü 60 $ tüm ücretlerin üst %0,1'indedir.
- B) Global IQR yakalar, çünkü IQR her zaman bağlamı dikkate alır.
- C) Hiçbir yöntem yakalayamaz, çünkü ücret pozitif olduğu için geçerlidir.
- D) Yalnızca "ücret ≤ 0" kuralı yakalar.
- E) Güzergâh bazlı (window ile aynı biniş→iniş bölgesi içinde hesaplanan) bağlamsal anomali yakalar; 60 $ tüm veride nadir değildir ama kendi güzergâhının dağılımına göre aşırıdır.

---

## Yedek sorular

**Soru 5 (Öğrenci 1 — column pruning).** 20 kolonluk veriden yalnızca 3 kolon okunduğunda Spark'ta Parquet okuması 3,8 kata kadar hızlanırken CSV okuması en fazla 2 kat hızlanmıştır. Bunun nedeni nedir?

- A) Parquet her kolonu dosyada ayrı bloklar halinde tutar ve konumlarını metadata'da saklar; istenmeyen kolonlar diskten hiç okunmaz. CSV'de ise bir satırdaki kolona ulaşmak için satırın tamamı taranmak zorundadır.
- B) CSV dosyaları 3 kolondan fazlasını desteklemez.
- C) Spark CSV dosyalarını her zaman tek çekirdekle okur.
- D) Parquet seçilmeyen kolonları dosyadan kalıcı olarak siler.
- E) CSV'de kolon seçimi yapılamaz; her zaman 20 kolon döner.

---

**Soru 6 (Öğrenci 2 — temizliğin etkisi).** Temizlikten sonra yolculuk mesafesinin **ortalaması** 5,74 milden 3,25 mile düşmüş, **medyanı** ise 1,83'ten 1,84'e neredeyse hiç değişmemiştir. Bu durum en iyi nasıl açıklanır?

- A) Temizlik uzun yolculukların tamamını sildiği için tipik yolculuk kısalmıştır.
- B) Ortalama her değeri eşit ağırlıkla topladığı için birkaç aşırı uç değerden (örn. 328.522 mil) çok etkilenir; medyan yalnızca sıralamaya baktığı için etkilenmez. Temizlik dağılımın gövdesini değil, kuyruğunu değiştirmiştir.
- C) Medyan hesabı hatalıdır; temizlikten sonra mutlaka değişmesi gerekirdi.
- D) Temizlik verinin yarısını sildiği için ortalama yarıya inmiştir.
- E) Ortalama ve medyan her zaman birlikte değişir; fark ölçüm hatasıdır.

---

## Cevap anahtarı

| Soru | Cevap | Açıklama |
|---|---|---|
| 1 | **B** | Kolon bazlı düzen, düşük çeşitlilikli kolonlarda (VendorID, payment_type, mta_tax…) dictionary ve RLE encoding'i çok etkili kılar; tarih ve sayılar metin yerine ikili tiplerle saklanır, ardından codec (zstd) uygulanır. Parquet kayıpsızdır ve satır atlamaz (A, C, D yanlış). |
| 2 | **D** | Projede bu nedenle Spark okuması `df.write.format("noop")` ile tetiklendi; bu, tüm kolonları gerçekten çözer. `count()` lazy değildir, bir action'dır (B yanlış). |
| 3 | **C** | Eksiklik rastgele değil: gözlenen bir değişkene (`payment_type = 0`) bağlı, yapısal bir boşluk — ön fiyatlı yolculukta taksimetre bu alanları doldurmuyor. `dropna()` verinin %25,3'ünü (4,8M kayıt) yok ederdi; ortalama veya 0 ile doldurmak Flex Fare yolculuklarını yanlışlıkla standart gösterirdi. |
| 4 | **E** | 60 $ global olarak sıradan bir ücrettir (p99,9 = 143 $); global IQR ise taksi verisinin ağır kuyruğuna uymaz ve bağlamı bilmez. Projede güzergâh bağlamı global eşiğin ~15 katı ücret anomalisi buldu ve ikisinin örtüşmesi çok düşüktü. |
| 5 | **A** | Column pruning Parquet'in kolon bazlı düzeninden gelir. CSV'de kazanç yalnızca okunan metnin sayıya çevrilmemesinden gelir; dosyanın tamamı yine okunur. |
| 6 | **B** | Medyan uç değerlere dayanıklıdır (robust), ortalama değildir. Temizlenmemiş veride ortalamaya dayanan her sonuç yanıltıcıdır. |
