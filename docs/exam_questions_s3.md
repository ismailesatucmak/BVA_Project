# Öğrenci 3 — Sınav soruları

**1.** `trips.groupBy("pickup_hour").count()` tam temiz veri üzerinde çalıştırıldığında saat 18 için dönen sayı neyi gösterir?

- A) Her gün saat 18'de başlayan yolculukların günlük ortalamasını.
- B) Dönem boyunca 18.00–18.59 arasında sona eren yolculukların toplamını.
- C) Dönem boyunca 18.00–18.59 arasında başlayan yolculukların toplamını.
- D) Saat 18'de başlayan yolculukların toplam süresini.
- E) Saat 18'de başlayan yolculukların görüldüğü farklı gün sayısını.

**2.** Biniş bölgesi koduna göre sayılan yolculuklar ile TLC bölge tablosu birleştiriliyor. TLC tablosunda bir kod için eşleşme bulunmazsa sol birleştirme (`left join`) nasıl davranır?

- A) Bölge kodu ile sayısını korur, bölge adını boş bırakır.
- B) Eşleşmeyen kodun sayısını siler.
- C) Eşleşmeyen kodu en yoğun bölgeye ekler.
- D) Bütün bölgelerin sayısını sıfırlar.
- E) Eşleşmeyen kodu ödeme türü olarak işaretler.

**Cevaplar.** 1-C · Veri biniş saatine göre gruplanır. 2-A · Sol birleştirme sol tablodaki grubu korur ve eşleşme yoksa bölge bilgisi boş kalır.
