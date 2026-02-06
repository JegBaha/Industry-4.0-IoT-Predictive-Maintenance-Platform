
# Endüstriyel Alarm Yönetimi Rehberi

## ISA-18.2 / IEC 62682 - Alarm Yönetim Standardı

### Alarm Öncelik Seviyeleri
| Seviye | Yanıt Süresi | Durum | Renk |
|--------|-------------|-------|------|
| Critical | Anında (<1 dk) | Tehlike, can/mal güvenliği | Kırmızı |
| High | <5 dakika | Üretim kaybı, ekipman hasarı | Turuncu |
| Medium | <15 dakika | Proses sapması | Sarı |
| Low | <60 dakika | Bilgilendirme | Mavi |

### Alarm Rasyonalizasyonu
Her alarm şu soruları geçmelidir:
1. Operatör müdahalesi gerekiyor mu?
2. Müdahale için yeterli zaman var mı?
3. Sonuçları ciddi mi?
4. Alarm olmadan fark edilemez mi?

### Alarm Flood Yönetimi
- Hedef: Operatör başına saatte <6 alarm
- Alarm shelving: Geçici bastırma (max 24 saat)
- Standing alarm: Sürekli aktif alarm (araştırılmalı)

## Smart Factory Alarm Eşikleri

### Sıcaklık (Motor/Rulman)
| Makine Tipi | Warning (°C) | Critical (°C) | Durdurma (°C) |
|-------------|-------------|---------------|---------------|
| Elektrik Motoru | 70 | 85 | 100 |
| Rulman | 65 | 80 | 95 |
| Hidrolik Sistem | 55 | 65 | 75 |
| Kompresör | 80 | 100 | 120 |

### Titreşim (RMS Velocity mm/s) - ISO 10816 bazlı
| Makine Sınıfı | Warning | Critical | Durdurma |
|---------------|---------|----------|----------|
| Sınıf I (<15kW) | 2.8 | 4.5 | 5.6 |
| Sınıf II (15-75kW) | 4.5 | 7.1 | 8.9 |
| Sınıf III (Büyük-rijit) | 7.1 | 11.2 | 14.0 |
| Sınıf IV (Büyük-esnek) | 9.0 | 14.0 | 17.5 |

### Akım (Motor)
| Durum | Eşik |
|-------|------|
| Normal | Nominal ±10% |
| Warning | Nominal +20% |
| Critical | Nominal +50% |
| Topraklama Kaçağı | >30 mA |

### Basınç
| Sistem | Warning | Critical |
|--------|---------|----------|
| Hidrolik | -10%/+15% nominal | -20%/+25% nominal |
| Pnömatik | <5.5 bar / >7.5 bar | <5.0 bar / >8.0 bar |

## Alarm → OEE Etkisi

### Alarm'ın OEE'ye Etkisi
| Alarm Türü | Etkilenen OEE Faktörü | Etki |
|-----------|----------------------|------|
| Makine durma | Availability | Doğrudan düşüş |
| Hız azaltma | Performance | Throughput düşer |
| Kalite sapma | Quality | Hurda/rework artar |
| Sensör arızası | Hepsi | Güvenilmez veri |

### Root Cause Analizi (Kök Neden)
| Belirti | Olası Neden | Kontrol |
|---------|-------------|---------|
| Yüksek titreşim + sıcaklık | Rulman arızası | Yağ analizi, vibrasyon spektrumu |
| Yüksek akım + düşük hız | Mekanik yük artışı | Kayış/dişli kontrolü |
| Düşük throughput + normal sensörler | Malzeme sorunu | Hammadde kalitesi |
| Titreşim 1× RPM dominant | Dengesizlik | Balans ayarı |
| Titreşim 2× RPM + eksenel | Hizasızlık | Lazer alignment |
