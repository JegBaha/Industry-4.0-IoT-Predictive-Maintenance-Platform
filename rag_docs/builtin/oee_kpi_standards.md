
# OEE ve Üretim KPI Standartları

## OEE (Overall Equipment Effectiveness)

### Tanım (ISO 22400 / SEMI E10)
OEE = Availability × Performance × Quality

### Bileşenler

#### 1. Availability (Kullanılabilirlik)
Availability = Operating Time / Planned Production Time × 100

- Planned Production Time = Toplam Süre - Planlı Duruşlar
- Operating Time = Planned Production Time - Plansız Duruşlar
- Duruş Kategorileri:
  - Planlı: Mola, bakım, setup, temizlik
  - Plansız: Arıza, malzeme bekleme, operatör bekleme

#### 2. Performance (Performans)
Performance = (Ideal Cycle Time × Total Count) / Operating Time × 100

- Ideal Cycle Time = tasarım hızında üretim süresi
- Hız kayıpları: yavaş çalışma, küçük duruşlar (<5 dk)

#### 3. Quality (Kalite)
Quality = Good Count / Total Count × 100

- Hurda (scrap)
- Yeniden işleme (rework)
- İlk geçişte doğru üretim (First Pass Yield)

### World Class OEE Değerleri
| Metrik | World Class | Typical |
|--------|------------|---------|
| Availability | >90% | 85% |
| Performance | >95% | 90% |
| Quality | >99% | 95% |
| **OEE** | **>85%** | **60%** |

### Six Big Losses (Altı Büyük Kayıp - TPM)
1. Arıza Kayıpları (Equipment Failure) → Availability
2. Setup/Ayar Kayıpları (Setup & Adjustments) → Availability
3. Küçük Duruş Kayıpları (Minor Stops) → Performance
4. Hız Kayıpları (Reduced Speed) → Performance
5. Üretim Hata Kayıpları (Production Rejects) → Quality
6. Başlangıç Kayıpları (Startup Rejects) → Quality

## MTBF & MTTR

### MTBF (Mean Time Between Failures)
MTBF = Toplam Çalışma Süresi / Arıza Sayısı

Örnek: 720 saat çalışma, 3 arıza → MTBF = 240 saat

### MTTR (Mean Time To Repair)
MTTR = Toplam Tamir Süresi / Arıza Sayısı

Örnek: 12 saat toplam tamir, 3 arıza → MTTR = 4 saat

### Availability (MTBF ile)
Availability = MTBF / (MTBF + MTTR)

### Hedef Değerler (Endüstriyel)
| Metrik | İyi | Çok İyi |
|--------|-----|---------|
| MTBF | >200 saat | >500 saat |
| MTTR | <4 saat | <1 saat |
| MTBF/MTTR Ratio | >50 | >500 |

## ISA-95 (IEC 62264) - ERP-MES Entegrasyonu

### Seviyeleri
- Level 0: Fiziksel süreç (sensörler, aktüatörler)
- Level 1: Temel kontrol (PLC, DCS)
- Level 2: Proses kontrol (SCADA, HMI)
- Level 3: Üretim operasyonları (MES, Batch)
- Level 4: İş planlama (ERP)

### KPI Hiyerarşisi (ISO 22400)
- Level 4: Finansal KPI'lar (maliyet, kâr)
- Level 3: Üretim KPI'ları (OEE, yield, throughput)
- Level 2: Proses KPI'ları (çevrim süresi, setup süresi)
- Level 1: Ekipman KPI'ları (MTBF, MTTR, alarm sayısı)
