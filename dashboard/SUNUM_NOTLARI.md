# Sunum notları (yalnızca hazırlık, dashboard'da görünmez)

Toplam süre ~26 dk: Giriş 3 · her öğrenci 4–5 · Teşekkürler 1. Gezinme: ← → panel, F tam ekran.

**Sunumdan önce:** projeksiyonda bir kez F'ye basıp tam ekranı deneyin. Açılış: `README.md` → "Sunum günü".
**Süre uyarısı:** Mehmet Buğra TEKİN panelinde 3 simülasyon var. Yalnızca groupBy simülasyonunu canlı çalıştırın; window ve haritayı "Notebook sonucu" modunda gösterip haritada birkaç bölgenin üzerine gelmek yeterli.
Tüm rakamlar `reports/results/*.csv`, notebook çıktıları ve `docs/technical_report_*.md`'den.

---

## 1. Giriş (~3 dk) · konuşan: ekip adına bir kişi

**Akış**
1. Problem ve araştırma sorusu: "Veri büyüdükçe klasik yaklaşım ne zaman yetersiz kalıyor?"
2. Neden Big Data: 18.999.282 kayıt, CSV 1.907 MB → Parquet zstd 290 MB; ayda ~3,1–3,7M temiz kayıt; pandas 19M CSV'yi 71 sn'de okuyor, 9,4 GB RAM tutuyor.
3. Mimari: 5 katman (kaynak → depolama → PySpark işleme → analiz/ML → dashboard), her kutuda sorumlu kişi.
4. Görev dağılımı ve çalışma ortamı (PySpark 3.5.9, pandas 2.3.3; i7-13700H 20 iş parçacığı, 32 GB).

**Olası sorular**
- *Bu veri gerçekten "büyük" mü?* 19M kayıt tek makinede sığıyor ama pandas'ın sınırına yakın: CSV okurken 9,4 GB RAM. Ayrıca 57M sentetik veriyle (CSV 5.722 MB) ölçeği üç katına çıkardık.
- *Neden küme değil tek makine?* Ders ortamı; Spark `local[*]` ile aynı API ve aynı plan çalışıyor. Çok düğümlü ölçekleme test edilmedi (bunu açıkça söyleyin).
- *Sentetik veri sonucu bozmaz mı?* 19M gerçek veri 3 kez blok blok kopyalandı; dosya oranı 2,999×. Dağılım aynı, yalnız hacim büyüdü.
- *Neden iki farklı makine?* Öğrenci 1'in okuma ölçümleri 28 çekirdek/16 GB makinede alındı; diğerleri i7-13700H/32 GB'da. Mutlak süreler makineler arasında karşılaştırılmıyor, her deney kendi içinde karşılaştırıldı.

---

## 2. Emir Han GÜRDAMAR · Veri ve formatlar (~4 dk)

**Akış**
1. Kod + gerçek çıktı: tek Spark yazıcısı 19M satırı 4 formatta yazıyor (CSV 1.907 MB, Parquet snappy 392 MB, 4 row group).
2. Veri sözlüğü: 20 kolon, imkânsız değerler (328.522 mil, 2001 yılı) → temizlik gerekçesi.
3. Disk: Parquet 5–6,6 kat küçük.
4. Okuma: pandas CSV 71 sn, Parquet 1,8 sn (~40×).
5. Bellek: 1,9 GB CSV → 9,4 GB RAM; 16 GB RAM ~32M kayıtta dolar.

**Olası sorular**
- *Neden Parquet daha hızlı/küçük?* Kolon bazlı, tipli ikili format; dictionary/RLE kodlama ve sıkıştırma. CSV her satırı metinden çözmek zorunda; Parquet yalnızca istenen kolonları okur (3 kolon okumada Spark'ta 3,8×'e kadar hızlanma).
- *Bellek neden disk boyutunun ~5 katı?* Metin sayıya, tarih nesnelerine, string kolonlar Python nesnelerine dönüşüyor; okuma sırasında ara tamponlar da tutuluyor.
- *gzip/zstd mi snappy mi?* zstd/gzip ~%25 daha küçük ama açması biraz daha yavaş; hız öncelikli işlemde snappy kullandık.
- *Veri sözlüğündeki istatistikler örneklem mi?* Hayır, 18.999.282 kaydın tamamından Spark ile hesaplandı.

---

## 3. İsmail Esat UÇMAK · Temizlik ve anomali (~4–5 dk)

**Akış**
1. Kod: 10 kural (R1–R10), eşikler kodda tanımlı. Gerçek çıktı: 1.788.321 kayıt silindi, 17.210.961 kaldı; `dropna()` olsaydı 4.812.280 kayıt gidecekti.
2. Huni: R7 (RatecodeID 99) 632.999 ve R2 (mesafe) 574.749 en büyük; toplam %9,4.
3. Simülasyon: eşikleri canlı değiştirip silinen oranı göster.
4. Öncesi/sonrası: medyan aynı (1,83 → 1,84 mil), ortalama ve maksimum düştü.
5. Isı haritası: hatalar sağlayıcıda yoğun (Helix %100 R4; Creative Mobile %17,3 R7 → Bronx kayıtlarının %54'ü, Brooklyn'in %30'u R7 ile siliniyor).

**Olası sorular**
- *Aykırı değerleri neye göre belirlediniz?* İki yaklaşım: (1) Kural: fiziksel/iş kuralı, "imkânsız mı?" (mesafe ≤ 0 veya > 100 mil, süre ≤ 1 veya > 180 dk, hız > 80 mph, ücret ≤ 0). (2) İstatistik: IQR ve p99,9, "nadir mi?". Silmek için yalnızca kuralı kullandık; istatistik ağır kuyruklu ücrette 1,25M meşru kaydı aykırı sayıyordu.
- *Neden dropna() kullanmadınız?* Flex Fare (payment_type 0) kayıtlarında bazı kolonlar tasarım gereği boş; dropna verinin %25,3'ünü (4.812.280) yanlışlıkla silerdi.
- *R9 ve R10 neden sıfır?* Tam kopya kayıt yok; R10'un yakaladıklarını R3 (negatif ücret) zaten silmişti.
- *Temizlik sonucu değiştirdi mi?* Gövde aynı, yalnız kuyruk kesildi: mesafe ortalaması 5,74 → 3,25 mil, medyan 1,83 → 1,84.

---

## 4. Mehmet Buğra TEKİN · PySpark aggregation (~5 dk)

**Akış**
1. Dört boyut şeridi: saat 18.00 (1.150.635), Perşembe 21.00, Upper East Side South (815.297), kart %64,96.
2. Kod: `groupBy("pickup_hour").count()`; gerçek çıktı + "Altı PySpark sonuç tablosu ve toplamlar doğrulandı."
3. groupBy simülasyonu: map → shuffle → reduce, bölümlü sonuç = tek parça.
4. İleri analitik: window (7 günlük ortalama, lag 7) ve join (bölge sözlüğü; broadcast 2,806 sn vs sort-merge 3,355 sn).
5. Harita ve ödeme türü.

**Olası sorular**
- *groupBy Spark'ta nasıl çalışıyor?* Her bölüm kendi kısmi sayımını yapar (partial_count), anahtara göre shuffle edilir, sonra birleştirilir (HashAggregate). Plan notebook'ta gösterildi.
- *Broadcast join neden daha hızlı?* 265 satırlık bölge tablosu her göreve kopyalanır; büyük tablo yeniden dağıtılmaz (shuffle yok). Deneyde ortanca 2,806 vs 3,355 sn, 4 turda sonuçlar eşleşti.
- *23 Şubat neden düşük?* 22–23 Şubat kar fırtınası; dönemin en düşük günü.
- *Window fonksiyonu ne ekliyor?* Satırları silmeden komşu satırlara bakar: son 7 günün ortalaması ve geçen haftanın aynı günüyle fark.

---

## 5. Hüseyin GÜLCAN · ML: süre tahmini (~4–5 dk)

**Akış**
1. Kod + çıktı: aynı 100K veride PySpark ve scikit-learn doğrusal modeli → RMSE 7,2018 vs 7,2007 (adil karşılaştırma).
2. Metrik tablosu (17M): doğrusal referans RMSE 7,16; Spark GBT 5,45, sklearn HGB 5,54; ayarlı model 5,04 (5M test).
3. RMSE–boyut grafiği: doğrusal model veriyle iyileşmiyor, ağaç modeli iyileşiyor.
4. Özellik önemi: mesafe 0,47, bölgeler 0,31.
5. Sızıntı kontrolü ve ayar araması; simülasyonda canlı eğitim.

**Olası sorular**
- *Modelin hatası ne anlama geliyor?* RMSE 5,45 dk: tahminler ortalama ~5,5 dakika sapıyor, büyük hatalar daha çok cezalandırılıyor. MAE 3,55 dk: tipik mutlak sapma. R² 0,80: süredeki değişkenliğin %80'i açıklanıyor.
- *Baseline nedir?* Doğrusal regresyonu basit referans olarak kullandık (RMSE 7,16, R² 0,65). Ortalama-tahmin (naif) baseline ölçülmedi.
- *Ücret kolonlarını neden çıkardınız?* Ücret süreden hesaplanıyor (sızıntı). Çıkarınca hata artmadı (5,561 → 5,536), yani model ücrete dayanmıyordu.
- *Spark neden ağaç modelinde yavaş?* sklearn HGB histogram tabanlı ve çok çekirdekli; Spark GBT her ağaçta dağıtık geçiş ve görev başlatma maliyeti ödüyor (sklearn 14–510× hızlı). Doğrusal modelde tersi: Spark 17M'de 10,8× hızlı.

---

## 6. Hüseyin GÜLCAN · Performans karşılaştırması (~5 dk)

**Akış**
1. Kod + çıktı: 1M kayıtta aynı 3 işlem iki motorda; satır sayıları ve checksum'lar eşit.
2. Karşılaştırma tablosu: 19M'de pandas + CSV 23,6 sn, Spark + CSV 22,2 sn; pandas + Parquet 5,3 sn (4,5×).
3. Uçtan uca ve işleme göre süre: join'de Spark önde, window'da geride.
4. 57M döküm: pandas + Parquet 21,4 sn, önbellekli Spark 36,8 sn.
5. Dosya düzeni simülasyonu ve araştırma sorusunun cevabı.

**Olası sorular**
- *Spark küçük veride neden daha yavaş?* Oturum açılışı ~5,5 sn, her işlemde planlama ve görev başlatma maliyeti var; 1M'de pandas + CSV 1,34 sn, Spark + Parquet 7,23 sn.
- *İki motor gerçekten aynı işi mi yaptı?* Evet, tüm motor × format × boyut kombinasyonlarında checksum'lar eşleşti.
- *Spark window'da neden yavaş?* `partitionBy` shuffle gerektiriyor (satırlar diske yazılıp yeniden okunuyor) ve 19M Parquet 3 row group olduğu için yalnızca 3 görevle okunuyor; 23 row group'la 6,1 → 3,85 sn.
- *cache kullansaydınız?* Ölçtük: işlemler hızlanıyor ama önbelleğe alma ~9 sn; ancak tekrarlanan sorgularda kâra geçiyor.
- *57M'de neden düz Spark yok?* 57M'de yalnızca pandas ve önbellekli Spark ölçüldü (tabloda not var). Önbellekli Spark süresine açılış ve önbelleğe alma dahil olduğundan karşılaştırma Spark aleyhine değil.
- *Notebook'taki eski HTML dashboard ne?* `reports/dashboard/index.html`, `src/dashboard.py`'nin ürettiği statik sonuç sayfası; sunumdaki bu dashboard aynı `reports/results/*.csv` dosyalarını interaktif olarak gösteriyor.

---

## 7. Teşekkürler (~1 dk)

Tek cümle sonuç: "Veri büyüdükçe önce CSV okuma, sonra bellek darboğaz oluyor; formatı değiştirmek motoru değiştirmekten daha etkili, ama RAM dolduğunda pandas çalışamaz." Ardından sorular.
