# Öğrenci 5 — Sınav Soruları

**1.** 19M kayıtlık NYC Taxi verisinde, tek makinede (20 iş parçacığı) iki işlem ölçülüyor. Yolculukları 265 satırlık bölge tablosuyla birleştirip ilçeye göre özetlemek (join) pandas'ta 2,1 sn, Spark + Parquet'te 0,7 sn sürüyor. Her biniş bölgesinde en yüksek tutarlı 3 yolculuğu bulmak (`row_number() over (partition by PULocationID ...)`) ise pandas'ta 2,1 sn, Spark'ta 6,1 sn sürüyor. Bu farkın en doğru açıklaması hangisidir?

- A) Spark join işlemlerini GPU üzerinde yapar; window fonksiyonlarını ise yalnızca tek çekirdekte çalıştırabilir.
- B) join'de Spark küçük tabloyu görevlere yayınlar (broadcast hash join) ve birleştirmeyi kısmi toplamayla 20 iş parçacığında yapar; birleşik tabloyu oluşturmaz. pandas ise 19M satırlık birleşik kopyayı oluşturup sonra gruplar. window ise tüm satırların bölgeye göre yeniden dağıtılmasını (shuffle: diske yazma + okuma) ve her bölümde sıralamayı gerektirir; pandas aynı işi tek bir bellek içi sıralamayla yapar.
- C) pandas window fonksiyonlarını desteklemediği için sonuç yaklaşık hesaplanmıştır; bu yüzden daha hızlıdır.
- D) Spark'ın window işlemi yavaştır çünkü Parquet dosyası satır bazlı bir formattır.
- E) İki işlemin sonuçları farklı olduğundan süreler karşılaştırılamaz.

**2.** Aynı 19M kayıt üzerinde saat bazlı `groupBy` işlemi, ön bellek (`cache`) kullanılmadan Spark ile CSV dosyasından 5,9 sn, Parquet dosyasından 0,9 sn sürüyor. pandas'ta ise veri bir kez belleğe yüklendikten sonra format fark etmeksizin işlem 0,7 sn sürüyor. Bu gözlem için hangisi doğrudur?

- A) Spark CSV'yi Parquet'e göre daha az paralel okur, çünkü CSV dosyaları bölünemez.
- B) Spark tembel çalıştığı için her işlem dosyayı yeniden okur. CSV'de her satırdaki tüm metnin parse edilip tiplere çevrilmesi gerekir; Parquet ise sütunlu, ikili ve sıkıştırılmış olduğu için yalnızca gereken kolonlar okunur. pandas'ta parse maliyeti yalnızca bir kez, yükleme aşamasında ödenir (19M CSV ~18,8 sn, Parquet ~0,4 sn).
- C) Parquet dosyası CSV'den daha fazla satır içerdiği için daha hızlı işlenir.
- D) pandas'ın groupby işlemi Spark'tan her zaman daha hızlıdır; format bu sonucu etkilemez.
- E) Spark'ın Parquet okuması `count()` ile metadata'dan cevap verdiği için veri hiç okunmamıştır.

---

**Cevaplar.**

**1 — B.** Bu projede ölçülen sonuç işlem türünün belirleyici olduğunu gösteriyor. Spark join'de 4,5M kayıttan itibaren pandas'ı geçiyor, window'da 19M'de hâlâ ~3× geride.
- A yanlış: Spark CPU üzerinde çalışır. Window da paralel çalışır, ama önce shuffle gerekir.
- C yanlış: pandas aynı sonucu `sort_values` + `groupby.head(3)` ile tam olarak üretir. Proje, iki motorun sonuçlarını checksum ile doğruladı.
- D yanlış: Parquet sütunlu (columnar) bir formattır.
- E yanlış: sonuçlar aynıdır ve doğrulanmıştır.

**2 — B.** Format, motordan daha belirleyici. Spark + Parquet groupby'ı Spark + CSV'den 6,4× hızlıdır. pandas'ta CSV'nin cezası yükleme aşamasında ödenir: 19M CSV'yi okumak Parquet'e göre 51× yavaştır.
- A yanlış: Spark sıkıştırılmamış CSV'yi de parçalara bölerek paralel okur.
- C yanlış: iki dosyada aynı satırlar vardır.
- D yanlış: Spark + Parquet join'de pandas'tan hızlıdır (soru 1).
- E yanlış: işlem `groupBy` + `collect` ile tetiklenmiştir, veri gerçekten okunmuştur. `count()` kısayolu Öğrenci 1'in okuma ölçümünde ele alınan bir tuzaktır.
