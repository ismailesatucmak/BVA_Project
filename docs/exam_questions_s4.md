# Öğrenci 4 — Sınav Soruları

**1.** Bir PySpark MLlib pipeline'ında bölge kodları `StringIndexer(handleInvalid="keep")` ve ardından `OneHotEncoder(dropLast=True)` ile kodlanıyor. Test kümesinde, eğitimde hiç görülmemiş bir bölge kodu çıkarsa ne olur?

- A) `StringIndexer` hata fırlatır ve pipeline durur.
- B) Kod, ek bir "bilinmeyen" indeksine atanır. Bu indeks one-hot'ta düşürülen son kategoriye denk geldiği için satır sıfır vektörüyle temsil edilir ve pipeline çalışmaya devam eder.
- C) Kod, eğitimde en sık görülen kategoriye atanır.
- D) O satır test kümesinden sessizce silinir.
- E) `OneHotEncoder` aşaması atlanır ve ham tamsayı doğrudan modele verilir.

**2.** Aynı 5M kayıtlık örneklemde, aynı train/test ayrımı, aynı özellikler ve aynı hiperparametrelerle (50 ağaç, derinlik 5) iki ölçüm yapılıyor. scikit-learn `HistGradientBoostingRegressor` 4,8 sn sürüyor ve 2,2 GB tepe bellek kullanıyor. PySpark `GBTRegressor` (`local[*]`, tek makine) 197 sn sürüyor ve süreç ağacının tepe belleği 4,2 GB. Bu sonucun en doğru yorumu hangisidir?

- A) PySpark'ın gradyan artırma algoritması matematiksel olarak daha karmaşık olduğu için yavaştır; büyük veride de her zaman yavaş kalır.
- B) Ölçüm hatalıdır. Spark yalnızca Python sürücüsü üzerinden ölçülmeli; o zaman Spark'ın belleği çok daha düşük çıkar.
- C) Veri RAM'e sığdığı sürece tek makinede sklearn'ün yerel (C++/Cython) çok iş parçacıklı uygulaması, Spark'ın iş zamanlama, JVM↔Python serileştirme ve görev başına sabit maliyetlerinden kurtulduğu için daha hızlıdır. Spark'ın JVM'i de sabit bir bellek tabanı ekler. Spark'ın getirisi veri tek makinenin RAM'ini aştığında veya küme gerektiğinde ortaya çıkar.
- D) scikit-learn veriyi diske taşıyarak (spill) çalıştığı için daha az bellek kullanır.
- E) 5M kayıt büyük veri sayılmadığı için Spark bu boyutta çalışamaz ve sonuçları güvenilir değildir.

---

**Cevaplar.**

**1 — B.** `handleInvalid="keep"` ile `StringIndexer` bilinmeyen değeri `numLabels` indeksine atar. Bu, bilinen etiketlerden sonraki ek bir kategoridir. `OneHotEncoder` varsayılan olarak `dropLast=True` ile son kategoriyi düşürdüğü için bu satır sıfır vektörüyle kodlanır, yani "hiçbir bilinen bölgeye ait değil" anlamına gelir.
- A yanlış: hata fırlatmak varsayılan `handleInvalid="error"` davranışıdır.
- C yanlış: en sık kategoriye atama yapan bir seçenek yoktur.
- D yanlış: satır silmek `handleInvalid="skip"` davranışıdır.
- E yanlış: pipeline aşamaları atlanmaz.

**2 — C.** Bu proje ölçümünde iki çerçeve aynı satırları, özellikleri ve ayarları kullandı. Doğrulukları da çok yakın çıktı (RMSE 5,48 vs 5,56 dk). Süre farkı Spark'ın dağıtık çalışma ek yükünden geliyor: her ağaç seviyesi ayrı bir iş olarak zamanlanıyor, histogramlar shuffle ile birleştiriliyor ve veri JVM ile Python arasında serileştiriliyor. Bu ek yükün göreli payı veri büyüdükçe azalıyor (100K'da ~110×, 5M'de ~40×).
- A yanlış: iki uygulama da histogram tabanlı gradyan artırmadır. Fark algoritmadan değil çalışma ortamından gelir.
- B yanlış: işi JVM yapar. Yalnızca Python sürecini ölçmek belleği gizler; projenin ilk sürümündeki hata buydu.
- D yanlış: sklearn diske taşma (spill) yapmaz, tüm veriyi RAM'de tutar.
- E yanlış: Spark her boyutta doğru çalıştı. Soru performansla ilgili, doğrulukla değil.
