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

**3 (yedek).** 19M kayıtlık Parquet verisinde Spark ile üç işlem (groupby, join, window) çalıştırılıyor. Önbelleksiz Spark'ta her işlem dosyayı yeniden okuyor ve üç işlem toplam 7,7 sn sürüyor. `df.cache().count()` ile veri önce Spark belleğine alındığında yükleme 8,9 sn, ardından üç işlem toplam 4,6 sn sürüyor. Aynı veri pandas'a Parquet'ten 0,4 sn'de yükleniyor. Bu tablodan çıkarılabilecek en doğru sonuç hangisidir?

- A) `cache()` her zaman toplam süreyi kısaltır; bu yüzden Spark'ta her DataFrame önbelleğe alınmalıdır.
- B) Önbellek işlemleri hızlandırır (7,7 → 4,6 sn), ama önbelleğe alma maliyeti (8,9 sn) yalnızca aynı veri üzerinde iş yükü birkaç kez tekrarlandığında amorti olur. Tek seferlik 3 işlemde önbelleksiz çalışma daha hızlıdır.
- C) Önbelleğe alınan veri diske yazıldığı için işlemler yavaşlar.
- D) pandas'ın yüklemesi hızlı olduğu için pandas işlemleri de her zaman Spark'tan hızlıdır.
- E) `count()` önbelleği doldurmaz; ölçülen 8,9 sn yalnızca metadata okumasıdır.

---

**Cevaplar.**

**1 — B.** Bu projede ölçülen sonuç işlem türünün belirleyici olduğunu gösteriyor. Spark join'de 4,5M kayıttan itibaren pandas'ı geçiyor (57M'de 8–9×), window'da 19M'de hâlâ ~3× geride. Farkın bir kısmı dosya düzeninden geliyor: aynı veri 3 yerine 23 row group'la yazılınca Spark window 6,1 → 3,85 sn (Spark bir row group'u bölemez). Kalan 1,85× fark shuffle + sıralama maliyetidir.
- A yanlış: Spark CPU üzerinde çalışır. Window da paralel çalışır, ama önce shuffle gerekir.
- C yanlış: pandas aynı sonucu `sort_values` + `groupby.head(3)` ile tam olarak üretir. Proje, iki motorun sonuçlarını checksum ile doğruladı.
- D yanlış: Parquet sütunlu (columnar) bir formattır.
- E yanlış: sonuçlar aynıdır ve doğrulanmıştır.

**2 — B.** Format, motordan daha belirleyici. Spark + Parquet groupby'ı Spark + CSV'den 6,4× hızlıdır. pandas'ta CSV'nin cezası yükleme aşamasında ödenir: 19M CSV'yi okumak Parquet'e göre 51× yavaştır.
- A yanlış: Spark sıkıştırılmamış CSV'yi de parçalara bölerek paralel okur.
- C yanlış: iki dosyada aynı satırlar vardır.
- D yanlış: Spark + Parquet join'de pandas'tan hızlıdır (soru 1).
- E yanlış: işlem `groupBy` + `collect` ile tetiklenmiştir, veri gerçekten okunmuştur. `count()` kısayolu Öğrenci 1'in okuma ölçümünde ele alınan bir tuzaktır.

**3 — B.** Önbellek, işlem başına maliyeti düşürür ama bir kerelik yükleme maliyeti ekler. Bu projede işlem başına kazanç 7,7 − 4,6 = 3,1 sn, yükleme 8,9 sn → iş yükü ~3 kez tekrarlanınca kâra geçer. CSV'de tersine ilk turda kâra geçer, çünkü önbelleksiz Spark CSV'yi her işlemde yeniden parse eder (16,7 sn → 4,4 sn).
- A yanlış: önbelleğin bellek ve yükleme maliyeti vardır; tek kullanımlık veride zarar ettirir.
- C yanlış: `cache()` önce belleği kullanır (`MEMORY_AND_DISK`); yalnızca sığmayan bölümler diske taşar ve yine de dosyayı yeniden okumaktan hızlıdır.
- D yanlış: önbellekteki Spark join'de pandas'tan 3–4× hızlıdır (Parquet: 0,61 vs 2,13 sn).
- E yanlış: `count()` bir eylemdir; tüm bölümleri hesaplayıp önbelleğe yazar. Metadata kısayolu yalnızca önbelleksiz Parquet `count()`'unda geçerlidir.
