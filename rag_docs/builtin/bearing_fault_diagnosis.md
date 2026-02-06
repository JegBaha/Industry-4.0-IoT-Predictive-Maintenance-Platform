
# Rulman Arıza Teşhisi (Bearing Fault Diagnosis)

## Rulman Arıza Frekansları
Rulman arızaları, specifik frekanslarda titreşim üretir:

### Temel Formüller
- BPFO (Ball Pass Frequency Outer) = n/2 × (1 - Bd/Pd × cos(α)) × RPM/60
- BPFI (Ball Pass Frequency Inner) = n/2 × (1 + Bd/Pd × cos(α)) × RPM/60
- BSF (Ball Spin Frequency) = Pd/(2×Bd) × (1 - (Bd/Pd × cos(α))²) × RPM/60
- FTF (Fundamental Train Frequency) = 1/2 × (1 - Bd/Pd × cos(α)) × RPM/60

Nerede:
- n = bilye sayısı
- Bd = bilye çapı
- Pd = pitch çapı
- α = temas açısı

## SKF Rulman Durum İzleme Rehberi

### Titreşim Seviyesi vs Arıza Durumu
| Seviye | Durum | Aksiyon |
|--------|-------|---------|
| Baseline | Normal çalışma | İzleme devam |
| 2× Baseline | Erken uyarı | İzleme sıklaştır |
| 4× Baseline | Arıza gelişiyor | Bakım planla |
| 8× Baseline | Ciddi arıza | Acil müdahale |
| 16× Baseline | Yıkıcı arıza riski | Makineyi durdur |

### Arıza Gelişim Aşamaları
1. **Aşama 1** (Ultrasonik bölge, >20kHz): Mikroskobik çatlaklar
2. **Aşama 2** (500Hz-20kHz): Doğal frekans rezonansları
3. **Aşama 3** (Rulman frekansları): BPFO/BPFI/BSF belirgin
4. **Aşama 4** (Geniş bant gürültü): Yaygın hasar, acil değişim

### Envelope Analizi (Zarf Analizi)
Rulman arıza tespitinde en etkili yöntem.
- Yüksek frekanslı rezonans bantlarını demodüle eder
- Düşük seviyeli rulman frekanslarını ortaya çıkarır
- SKF Enveloped Acceleration (gE) birimi

## IFM Electronic Vibration Sensor Alarm Tablosu
### VSE / VVB Serisi Sensörler
| Parametre | Warning | Alarm | Unit |
|-----------|---------|-------|------|
| Velocity RMS | 4.5 | 7.1 | mm/s |
| Acceleration Peak | 20 | 50 | m/s² |
| Displacement P-P | 50 | 100 | µm |
| Temperature | 70 | 85 | °C |
| Crest Factor | 5 | 8 | - |
