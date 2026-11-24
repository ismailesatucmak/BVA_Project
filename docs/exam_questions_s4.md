# Öğrenci 4 — Sınav Soruları

**1.** Bir PySpark MLlib pipeline'ında `StringIndexer(handleInvalid="keep")` ve `OneHotEncoder` kullanılarak `PULocationID` kolonundaki bölge kodları kodlanmaktadır. Eğitim setinde hiç görülmemiş bir bölge kodu test setinde karşılaşılırsa bu pipeline nasıl davranır?

- A) `NullPointerException` fırlatır ve pipeline durur.
- B) Bilinmeyen kodu ayrı bir "keep" kategorisi olarak sıfır vektörüyle temsil eder ve pipeline çalışmaya devam eder.
- C) Bilinmeyen kodu en yüksek frekanslı kategoriye atar.
- D) Bilinmeyen kodu içeren satırları filtreler ve test setinden siler.
- E) `StringIndexer` aşamasını atlayarak `OneHotEncoder`'ı doğrudan ham tamsayı üzerinde çalıştırır.

**2.** Yolculuk süresi tahmini için `scikit-learn` ile eğitilen bir `HistGradientBoostingRegressor` modeli 100K ve 1M kayıtta çalışırken 5M kayıtta ~3,13 GB RAM tüketmektedir; veri büyüklüğü makinenin fiziksel RAM sınırını aştığında (ör. tam 17,2M+ kayıt veya düşük bellekli bir ortam) süreç bellek hatasıyla (OOM) çökmektedir. Aynı donanımda PySpark `GBTRegressor` ise JVM belleği dolduğunda bile diske yazarak (spill) eğitimi tamamlamaktadır. Bu davranışın temel teknik açıklaması hangisidir?

- A) scikit-learn'ün gradyan artırma algoritması PySpark'ınkinden matematiksel olarak daha karmaşıktır, bu yüzden daha yavaştır.
- B) scikit-learn tüm veriyi aynı anda ana belleğe (RAM) yükler ve RAM dolduğunda çöker; PySpark ise veriyi bölümler hâlinde (partition) disk ve bellek arasında yöneterek RAM sınırını aşabilir.
- C) PySpark modelleri scikit-learn modellerinden çok daha küçük ağaçlar öğrenir, bu yüzden daha az bellek kullanır.
- D) scikit-learn paralel işlemeyi desteklemez; PySpark ise birden fazla çekirdek kullandığından bellek sorunu yaşamaz.
- E) 5M kayıt scikit-learn için makul olmayan bir veri büyüklüğüdür; bu yüzden scikit-learn'ün sağladığı fonksiyonlar bu kadar büyük veriyle çağrılamaz.

---

**Cevaplar.**

**1 — B.** `handleInvalid="keep"` parametresi sayesinde `StringIndexer`, bilinmeyen değerleri `numLabels` indeksine (eğitimde görülen en son indeksin bir fazlası) atar. `OneHotEncoder` bu indeksi sıfır vektörüyle kodlar; böylece pipeline çökmez, bilinmeyen bölge "hiçbir kategoriye ait değil" olarak temsil edilir. Seçenek A yanlış: `handleInvalid="error"` (varsayılan) hata fırlatır ama biz açıkça "keep" seçtik. Seçenek C yanlış: en yüksek frekanslı kategoriye atama `handleInvalid="skip"` değil, hiçbir seçenekte yoktur. Seçenek D, E yanlış: pipeline satır filtrelemez ve aşama atlamaz.

**2 — B.** scikit-learn tek makinede tüm veriyi RAM'e yükleyerek çalışır; veri RAM'e sığmadığında süreç bellek hatası verir ya da swap nedeniyle çok yavaşlar. PySpark, veriyi partition'lara bölerek disk tabanlı spill mekanizmasıyla RAM'den büyük veri setlerini işleyebilir. Seçenek A yanlış: her iki kütüphane de GBT türevi algoritmalar kullanır, matematiksel karmaşıklık farkı performans sınırını belirlemez. Seçenek C yanlış: ağaç derinliği ve sayısı bağımsız hiperparametredir. Seçenek D yanlış: scikit-learn `n_jobs=-1` ile paralel işleme destekler ama bu RAM sınırını kaldırmaz. Seçenek E yanlış: teknik bir kısıt değil, sadece yanlış bir genellemedir; scikit-learn çağrılabilir ama bellek tükenebilir.
