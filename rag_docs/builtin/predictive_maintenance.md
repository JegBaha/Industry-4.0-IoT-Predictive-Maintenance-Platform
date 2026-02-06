
# Prediktif Bakım (Predictive Maintenance) Rehberi

## Bakım Stratejileri Karşılaştırması
| Strateji | Açıklama | Maliyet | Etkinlik |
|----------|----------|---------|----------|
| Reactive | Arızalandığında tamir | Yüksek | Düşük |
| Preventive | Zamana dayalı bakım | Orta | Orta |
| Predictive | Duruma dayalı bakım | Düşük-Orta | Yüksek |
| Prescriptive | AI önerili bakım | Düşük | En yüksek |

## Condition Monitoring Teknikleri

### 1. Vibration Analysis (Titreşim Analizi)
- En yaygın CBM tekniği
- Dönen makineler için ideal
- FFT spektrum analizi ile arıza tespiti
- Trend analizi ile RUL (Remaining Useful Life) tahmini

### 2. Termografi (Infrared Thermography)
- Elektrik panoları, motorlar, rulmanlar
- Anormal sıcaklık dağılımı tespiti
- ΔT >15°C → Investigation gerekli
- ΔT >40°C → Acil müdahale

### 3. Yağ Analizi (Oil Analysis)
- Metal parçacık analizi (Fe, Cu, Al)
- Viskozite değişimi
- Nem oranı
- ISO 4406 temizlik sınıfı

### 4. Ultrasonik Test
- Yüksek frekans ses analizi
- Kaçak tespiti (basınçlı hava, buhar)
- Elektrik arkı tespiti
- Rulman lubrikasyon kontrolü

## P-F Curve (Potansiyel Arıza - Fonksiyonel Arıza)
```
Condition
  ^
  |  P (Potential Failure - İlk tespit edilebilir belirti)
  |     |    \  ← P-F Interval (müdahale penceresi)
  |       |      F (Functional Failure - Makine durur)
  +──────────────────→ Time
```

### P-F Interval Örnekleri
| Arıza Modu | P-F Interval | İzleme Sıklığı |
|-----------|--------------|-----------------|
| Rulman arızası | 1-9 ay | Haftalık-Aylık |
| Dişli aşınması | 1-6 ay | Aylık |
| Dengesizlik | 2-4 hafta | Haftalık |
| Misalignment | 1-3 ay | Aylık |
| Kavitasyon | 2-4 hafta | Haftalık |

## LSTM Tabanlı Arıza Tahmini

### Feature Engineering
Prediktif bakım için önerilen özellikler:
1. **İstatistiksel**: Mean, RMS, Std, Skewness, Kurtosis
2. **Frekans**: Dominant frekans, BPFO/BPFI, harmonikler
3. **Zaman**: Rolling mean, rate of change, trend
4. **Çapraz**: Sıcaklık × Titreşim korelasyonu

### Model Değerlendirme
| Metrik | Açıklama | Hedef |
|--------|----------|-------|
| Precision | Doğru alarm / Toplam alarm | >90% |
| Recall | Tespit edilen arıza / Gerçek arıza | >95% |
| F1-Score | Precision-Recall dengesi | >92% |
| Lead Time | Arıza öncesi uyarı süresi | >24 saat |
