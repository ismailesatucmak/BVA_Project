# NYC Taxi — sunum dashboard'u

7 panelli, tamamen çevrimdışı çalışan sunum: Giriş, beş öğrenci paneli, Teşekkürler.
React + Vite + ECharts. Bütün veriler `src/data/` altına gömülüdür; çalışırken internet gerekmez.

## İlk kurulum (bir kez, internet gerekir)

Node.js 20 veya üstü (geliştirmede v24 kullanıldı).

```
cd dashboard
npm install
npm run build
```

## Sunum günü (internetsiz)

```
cd dashboard
npm run preview
```

Tarayıcıda http://localhost:4173 adresini açın.

| Tuş | İşlev |
|---|---|
| ← → | önceki / sonraki panel |
| F | tam ekran |

Üstteki noktalar ve soldaki liste de panele götürür. Panel adresi `#ogrenci-3` gibi doğrudan açılabilir.

Başka bilgisayarda sunulacaksa `dashboard/` klasörünü `node_modules` ve `dist` dahil kopyalayın; o makinede `npm install` gerekmez, yalnızca `npm run preview` çalıştırın.

## Veriyi yeniden üretmek (isteğe bağlı)

Notebook'lar ve `reports/results/*.csv` değişirse:

```
python dashboard/scripts/extract_notebooks.py
python dashboard/scripts/extract_data.py --data-root <veri klasörü> --zones-zip <taxi_zones.zip>
python dashboard/scripts/sim_reference.py
cd dashboard && npm run verify:sim && npm run build
```

`verify:sim`, simülasyonlardaki JavaScript algoritmalarını Python referansıyla karşılaştırır.

## Dosyalar

| Yol | İçerik |
|---|---|
| `src/content/panels.js` | panel içerikleri: kartlar, açıklamalar, kod parçaları |
| `src/content/team.js` | ekip ve öğrenci numaraları |
| `src/components/Panels.jsx` | Giriş, öğrenci paneli ve Teşekkürler görünümleri |
| `src/sims/` | 6 interaktif simülasyon |
| `SUNUM_NOTLARI.md` | konuşma akışı ve olası sorular (yalnız hazırlık) |
