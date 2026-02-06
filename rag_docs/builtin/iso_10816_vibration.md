
# ISO 10816 - Titreşim Şiddeti Standartları

## Genel Bakış
ISO 10816, dönen makinelerin titreşim ölçümü ve değerlendirilmesi için uluslararası standarttır.
Geniş bantlı titreşim hızı (velocity RMS, mm/s) üzerinden makine durumu sınıflandırılır.
ISO 10816 serisi, 2016'dan itibaren ISO 20816 ile güncellenmiştir.

## ISO 10816-3: Endüstriyel Makineler (15 kW üzeri)

### Sınıf I - Küçük Makineler (15 kW'a kadar)
| Bölge | RMS Velocity (mm/s) | Durum           |
|-------|---------------------|-----------------|
| A     | 0 – 1.4             | Yeni makine     |
| B     | 1.4 – 2.8           | Kabul edilebilir|
| C     | 2.8 – 4.5           | Uyarı           |
| D     | > 4.5               | Tehlikeli       |

### Sınıf II - Orta Makineler (15–75 kW)
| Bölge | RMS Velocity (mm/s) | Durum           |
|-------|---------------------|-----------------|
| A     | 0 – 2.8             | Yeni makine     |
| B     | 2.8 – 4.5           | Kabul edilebilir|
| C     | 4.5 – 7.1           | Uyarı           |
| D     | > 7.1               | Tehlikeli       |

### Sınıf III - Büyük Makineler (rijit montaj)
| Bölge | RMS Velocity (mm/s) | Durum           |
|-------|---------------------|-----------------|
| A     | 0 – 3.5             | Yeni makine     |
| B     | 3.5 – 7.1           | Kabul edilebilir|
| C     | 7.1 – 11.2          | Uyarı           |
| D     | > 11.2              | Tehlikeli       |

### Sınıf IV - Büyük Makineler (esnek montaj)
| Bölge | RMS Velocity (mm/s) | Durum           |
|-------|---------------------|-----------------|
| A     | 0 – 4.5             | Yeni makine     |
| B     | 4.5 – 9.0           | Kabul edilebilir|
| C     | 9.0 – 14.0          | Uyarı           |
| D     | > 14.0              | Tehlikeli       |

## Ölçüm Noktaları
- Yatak yuvaları üzerinde (bearing housing)
- Yatay (H), Dikey (V) ve Eksenel (A) yönlerde
- Frekans aralığı: 10 Hz – 1000 Hz
- Ölçüm birimi: mm/s RMS (velocity)

## Alarm Eşikleri (Genel Endüstriyel Uygulama)
- Warning (Uyarı): Bölge B/C sınırı
- Danger (Tehlike): Bölge C/D sınırı
- Makine durdurma: Bölge D'nin %25 üstü

## Yaygın Titreşim Arıza Nedenleri
1. Dengesizlik (Unbalance) - 1× RPM dominant
2. Misalignment (Hizasızlık) - 2× RPM dominant, eksenel yüksek
3. Rulman arızası (Bearing fault) - Yüksek frekanslı spike'lar
4. Gevşeklik (Looseness) - Harmonikler (0.5×, 1×, 2×, 3× RPM)
5. Dişli arızası (Gear fault) - Dişli frekansi ve yan bantlar
6. Kavitasyon - Rastgele yüksek frekanslı gürültü
