# Öğrenci 4 — Sınav Soruları

**1.** Bir PySpark MLlib pipeline'ında bölge kodları `StringIndexer(handleInvalid="keep")` ve ardından `OneHotEncoder(dropLast=True)` ile kodlanıyor. Test kümesinde, eğitimde hiç görülmemiş bir bölge kodu çıkarsa ne olur?

- A) `StringIndexer` hata fırlatır ve pipeline durur.
- B) Kod, ek bir "bilinmeyen" indeksine atanır. Bu indeks one-hot'ta düşürülen son kategoriye denk geldiği için satır sıfır vektörüyle temsil edilir ve pipeline çalışmaya devam eder.
- C) Kod, eğitimde en sık görülen kategoriye atanır.
- D) O satır test kümesinden sessizce silinir.
- E) `OneHotEncoder` aşaması atlanır ve ham tamsayı doğrudan modele verilir.

**2.** Aynı 5M kayıtlık örneklemde, aynı train/test ayrımı, aynı özellikler ve aynı hiperparametrelerle (50 ağaç, derinlik 5) iki ölçüm yapılıyor. scikit-learn `HistGradientBoostingRegressor` 4,8 sn sürüyor ve 2,2 GB tepe bellek kullanıyor. PySpark `GBTRegressor` (`local[*]`, tek makine, veri 20 bölüme dağıtılmış) 141 sn sürüyor ve süreç ağacının tepe belleği 6,2 GB. Bu sonucun en doğru yorumu hangisidir?

- A) PySpark'ın gradyan artırma algoritması matematiksel olarak daha karmaşık olduğu için yavaştır; büyük veride de her zaman yavaş kalır.
- B) Ölçüm hatalıdır. Spark yalnızca Python sürücüsü üzerinden ölçülmeli; o zaman Spark'ın belleği çok daha düşük çıkar.
- C) Veri RAM'e sığdığı sürece tek makinede sklearn'ün yerel (C++/Cython) çok iş parçacıklı uygulaması, Spark'ın iş zamanlama, JVM↔Python serileştirme ve görev başına sabit maliyetlerinden kurtulduğu için daha hızlıdır. Spark'ın JVM'i de sabit bir bellek tabanı ekler. Spark'ın getirisi veri tek makinenin RAM'ini aştığında veya küme gerektiğinde ortaya çıkar.
- D) scikit-learn veriyi diske taşıyarak (spill) çalıştığı için daha az bellek kullanır.
- E) 5M kayıt büyük veri sayılmadığı için Spark bu boyutta çalışamaz ve sonuçları güvenilir değildir.

**3 (yedek).** 17M kayıtlık eğitim verisi tek bir Parquet dosyasına, 128 MB'lık row group'larla yazılmış (dosyada 2 row group var). Bu dosyayla 20 iş parçacıklı bir makinede `local[*]` modunda `GBTRegressor` eğitimi 990 sn sürüyor. Veri eğitimden önce `repartition(20)` ile dağıtılınca süre 269 sn'ye iniyor. Aynı işlem 100K kayıtta ise 23 sn'den 103 sn'ye çıkıyor. Bu iki sonucu birlikte en iyi açıklayan hangisidir?

- A) `repartition` veriyi sıkıştırdığı için büyük veride hızlandırır, küçük veride sıkıştırma oranı düşük olduğu için yavaşlatır.
- B) Spark bir row group'u bölemez; dosya 2 bölüm olarak okunur ve eğitim 2 görevde, yani 2 çekirdekte çalışır. 20 bölüm paralelliği artırır. Ama her ağaç seviyesi 20 görevli ayrı bir iş olduğundan görev başına sabit koordinasyon maliyeti doğar. Büyük veride hesap bu maliyeti aşar, küçük veride aşamaz.
- C) `repartition` veriyi sürücüye toplar ve tek çekirdekte işler; büyük veride bu daha verimlidir.
- D) Parquet dosyaları her zaman tek bölüm olarak okunur; bölüm sayısı ancak CSV'de artırılabilir.
- E) Fark ölçüm hatasıdır; Spark'ta bölüm sayısı performansı etkilemez.

---

**Cevaplar.**

**1 — B.** `handleInvalid="keep"` ile `StringIndexer` bilinmeyen değeri `numLabels` indeksine atar. Bu, bilinen etiketlerden sonraki ek bir kategoridir. `OneHotEncoder` varsayılan olarak `dropLast=True` ile son kategoriyi düşürdüğü için bu satır sıfır vektörüyle kodlanır, yani "hiçbir bilinen bölgeye ait değil" anlamına gelir.
- A yanlış: hata fırlatmak varsayılan `handleInvalid="error"` davranışıdır.
- C yanlış: en sık kategoriye atama yapan bir seçenek yoktur.
- D yanlış: satır silmek `handleInvalid="skip"` davranışıdır.
- E yanlış: pipeline aşamaları atlanmaz.

**2 — C.** Bu proje ölçümünde iki çerçeve aynı satırları, özellikleri ve ayarları kullandı. Doğrulukları da çok yakın çıktı (RMSE 5,47 vs 5,56 dk). Süre farkı Spark'ın dağıtık çalışma ek yükünden geliyor: her ağaç seviyesi 20 görevli ayrı bir iş olarak zamanlanıyor ve histogramlar görevlerden toplanıp birleştiriliyor (~100 sn sabit maliyet). Bu ek yükün göreli payı veri büyüdükçe azalıyor (100K'da ~500×, 5M'de ~29×, 17M'de ~14×).
- A yanlış: iki uygulama da histogram tabanlı gradyan artırmadır. Fark algoritmadan değil çalışma ortamından gelir.
- B yanlış: işi JVM yapar. Yalnızca Python sürecini ölçmek belleği gizler; projenin ilk sürümündeki hata buydu.
- D yanlış: sklearn diske taşma (spill) yapmaz, tüm veriyi RAM'de tutar.
- E yanlış: Spark her boyutta doğru çalıştı. Soru performansla ilgili, doğrulukla değil.

**3 — B.** Parquet'te en küçük okunabilir birim row group'tur. Tek dosya + 2 row group = 2 dolu bölüm (Spark UI'da tek/iki aktif görev, ~2,4 çekirdek CPU). `repartition(20)` 20 çekirdeği kullandırır: 17M'de 3,7× hızlanma. Ama GBT'de 50 ağacın her seviyesi ayrı bir iş ve her iş 20 görev başlatır; 100K ve 1M'de süre ~104 sn'de sabit kalır, yani bu koordinasyon maliyetidir. Doğru bölüm sayısı veri boyutuna bağlıdır.
- A yanlış: `repartition` sıkıştırma yapmaz, satırları bölümler arasında yeniden dağıtır (shuffle).
- C yanlış: veriyi sürücüye toplayan `collect`/`coalesce(1)` gibi işlemlerdir; `repartition(20)` tersine dağıtır.
- D yanlış: çok row group'lu veya çok dosyalı Parquet paralel okunur; sorun dosyanın düzenidir, formatın kendisi değil.
- E yanlış: aynı RMSE ile 3,7× fark, bölüm sayısının belirleyici olduğunu gösterir.
