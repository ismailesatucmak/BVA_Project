# Kısa Teknik Rapor — Öğrenci 1 ve Öğrenci 2

**Proje:** NYC Taxi Büyük Veri Analitiği · **Araştırma sorusu:** Veri büyüdükçe klasik analiz yaklaşımı ne zaman yetersiz kalıyor?
**Kapsam:** veri temini, veri sözlüğü, format dönüşümü ve veri büyüklüğü analizi (Öğrenci 1); temizlik ve anomali analizi (Öğrenci 2).

## 1. Problem ve veri

NYC Taxi & Limousine Commission'ın (TLC) yayımladığı Yellow Taxi yolculuk kayıtları kullanıldı: **2026 Ocak–Mayıs, 18.999.282 kayıt, 20 kolon** (5 Parquet dosyası, 310 MB). Her satır bir yolculuktur: biniş/iniş zamanı ve bölgesi, mesafe, ücret kalemleri, ödeme türü. Veriyi TLC değil, taksimetre sağlayıcıları üretir ve TLC doğruluğunu garanti etmez; bu yüzden temizlik zorunludur.

Veri, tek makinenin sınırlarını zorlayacak büyüklükte: CSV olarak 1,9 GB, pandas ile okunurken 9,4 GB RAM. Gerçek veri yeterli olduğu için sentetik çoğaltma yapılmadı.

## 2. Mimari ve teknoloji

PySpark 3.5 (yerel mod, 28 çekirdek, 16 GB RAM, Windows) ana işleme motorudur; pandas yalnızca "klasik yaklaşım" karşılaştırması için kullanıldı. Akış (bkz. `docs/architecture.png`): TLC → `src.ingest` → ham Parquet → (S1) boyut setleri + ölçüm / (S2) profil → temizlik → `data/clean` → Öğrenci 3/4/5. Tüm adımlar script ile tekrar üretilebilir, 67 testle doğrulanır.

## 3. Öğrenci 1 — Format ve veri büyüklüğü

**Yöntem.** Veri 5 boyutta (100K, 1M, 5M, 10M, 19M) ve 4 formatta (CSV; Parquet snappy, gzip, zstd) yeniden yazıldı. Örnekleme satır hash'i (xxhash64) ile yapıldı: sonuç makineden bağımsız ve setler iç içe, böylece ölçüm farkları yalnızca boyuttan gelir. Her boyut × format için pandas ve Spark ile tam ve 3 kolonluk okuma süresi ile pandas'ın tepe belleği ölçüldü (85 kombinasyon, 1 ısınma + 3 tekrar).

**Adil ölçüm için alınan önlemler:** her okuma aynı tipli tabloyu üretir (CSV'de tarihler parse edilir, Spark'a şema verilir); Spark okuması `noop` yazımıyla tetiklenir (`count()` Parquet'te veriyi okumaz, metadata'dan cevap verir); her pandas ölçümü ayrı süreçte çalışır; her okumanın satır sayısı doğrulanır.

**Sonuçlar (19M kayıt):**

| | Disk | Tüm kolonlar | 3 kolon | pandas tepe RAM |
|---|---|---|---|---|
| pandas + CSV | 1.907 MB | 71,1 sn | 35,5 sn | 9,4 GB (%59) |
| pandas + Parquet snappy | 392 MB | 1,8 sn | 0,6 sn | 5,8 GB |
| Spark + CSV | 1.907 MB | 8,1 sn | 4,5 sn | — |
| Spark + Parquet snappy | 392 MB | 3,7 sn | 1,3 sn | — |

**Neden?**
- *Parquet 5–6,6 kat küçük:* kolon bazlı depolama + dictionary/RLE encoding + ikili tipler (19 karakterlik tarih metni yerine 8 baytlık sayı).
- *pandas + CSV 40 kat yavaş:* CSV metindir; 380 milyon hücre karakter karakter parse edilir, tipler tahmin edilir, okuma tek çekirdekte yapılır. Parquet'te değerler tipli ve okuyucu çok iş parçacıklıdır.
- *Spark CSV'de 9 kat hızlı:* parse işini 28 çekirdeğe böler. Ama tek makinede ve RAM'e sığan veride pandas + Parquet, Spark + Parquet'ten bile hızlıdır (JVM ve görev planlama yükü).
- *Column pruning:* Parquet'te 3 kolon okumak 3,8 kata kadar hızlandırır (diğer kolonlar diskten okunmaz); CSV'de en fazla 2 kat (satırın tamamı yine taranır).
- *Şemasızlık:* Spark CSV'de `inferSchema` okumayı 2 katına çıkarır (ek tam tarama).

**Araştırma sorusuna cevap.** Spark oturum açılışı (11,4 sn) dahil edildiğinde pandas + CSV **~3,6M kayıttan** sonra Spark'ın gerisine düşer. pandas'ın tepe belleği satır başına ~0,5 KB ile doğrusal artar ve bu makinede **~32M kayıtta 16 GB RAM tükenir** (doğrusal ekstrapolasyon). Bu ölçekte en büyük kazanç **formattan** gelir; klasik yaklaşımın sert sınırı ise **bellektir** — RAM'e sığmayan veride parçalı ve paralel işleyen Spark zorunludur.

## 4. Öğrenci 2 — Temizlik ve anomali

**Profil.** 19M kayıtta yüzdelikler ve 14 mantıksal kontrol hesaplandı. Ortalama ile medyan arasındaki uçurum (mesafe: 5,7 vs 1,8 mil; en uç değer 328.522 mil) sorunun ince bir kuyrukta olduğunu gösterdi. **Eksik değerler sistematik:** null içeren 5 kolonun tamamı yalnızca `payment_type = 0` (Flex Fare, ön fiyatlı yolculuk) kayıtlarında boş (%25,3). Bu yüzden null tek başına silme nedeni sayılmadı ve doldurma yapılmadı.

**Temizlik.** 10 kural (negatif/sıfır ücret, sıfır mesafe, 1 dk altı veya 3 saat üstü süre, 80 mph üstü hız, kapsam dışı tarih, bilinmeyen tarife kodu ve bölge, tam kopya, negatif CBD ücreti). Kopya tespiti **window** fonksiyonuyla yapıldı. Kurallar örtüştüğü için her kuralın tek başına, sıralı ve "yalnız bu kural" etkisi ayrı raporlandı.

| | Kayıt |
|---|---|
| Ham | 18.999.282 |
| Silinen (en az bir kural) | 1.788.321 (%9,4) |
| **Temiz** | **17.210.961 (%90,6)** |
| Karşılaştırma: naif `dropna()` | 4.812.280 silerdi (%25,3) |

Temizlik medyanları değiştirmedi, ortalamaları değiştirdi (mesafe ortalaması 5,74 → 3,25 mil, medyan 1,83 → 1,84): temizlenmemiş veride ortalamaya dayanan her sonuç yanıltıcıdır.

**Anomali analizi.**
- *Kural ≠ istatistik:* R3 (ücret ≤ 0) ile p99,9 eşiği (> 143 $) arasında örtüşme **sıfır**: kural *imkânsızı*, istatistik *nadiri* bulur. Global IQR ağır kuyruklu ücret dağılımına uymaz; üst sınırı 52 $ çıkar ve havalimanı yolculukları dahil 1,25M kaydı anomali sayar.
- *Bağlam (window):* her güzergâh (biniş → iniş bölgesi) kendi ücret dağılımıyla karşılaştırıldı. Bu yöntem 272K ücret anomalisi buldu; global p99,9 yalnızca 17,7K buluyor ve örtüşme 4,8K. Örnek: aynı bölgede 0,15 mil için 650 $ (güzergâh medyanı 8 $).
- *Kaynak (join):* zone lookup ile ilçe eklendi, oranlar sağlayıcı/ilçe/saat bazında hesaplandı. Hataların büyük kısmı sağlayıcı kaynaklı: **Helix iniş zamanını hiç göndermiyor** (234.756 kaydın tamamı silindi), Creative Mobile tarife kodunu göndermiyor (Bronx kayıtlarının %54'ü bu yüzden silindi), negatif ücretler Curb'ün iade/ters kayıtları (%89'unun pozitif bir eşi var).

## 5. Sınırlamalar

- Tek makine; ölçümler sıcak işletim sistemi önbelleğiyle alındı. Yalnızca **okuma** ölçüldü; işleme (groupby/join) performansı Öğrenci 5'in kapsamında. 32M sınırı ölçüm değil ekstrapolasyondur.
- Proje kararıyla silinen iki grup büyük olasılıkla gerçek yolculuk: `RatecodeID = 99` (638K; Vendor 1, normal mesafe/ücret profili) ve sıfır mesafeli Flex Fare kayıtları (407K; medyan 17 dk). Silinmeleri dış ilçelerin ve Flex Fare'nin temiz verideki payını azaltır.
- Bağlamsal anomali eşikleri (IQR k=3, IQR alt sınırı medyanın %20'si, en az 30 yolculuk) bir seçimdir.

## 6. Teslimler

Kod `src/`, notebook'lar `notebooks/01–02`, ölçümler `reports/results/`, grafikler `reports/figures/01–10`, veri sözlüğü `docs/data_dictionary.md`, temizlik kuralları `docs/cleaning_rules.md`, mimari şema `docs/architecture.png`, kurulum ve çalıştırma `README.md`.
