"""
Knowledge Base - Hazır Endüstriyel Standart Dokümanlar
ISO 10816, OEE standartları, alarm eşikleri, bakım rehberleri
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

from .document_processor import DocumentProcessor, Document
from .vector_store import VectorStore

logger = logging.getLogger(__name__)


# ─── Yerleşik (built-in) bilgi bankası dokümanları ───────────────────────────

BUILTIN_DOCS: Dict[str, str] = {}

BUILTIN_DOCS["iso_10816_vibration"] = """
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
"""

BUILTIN_DOCS["bearing_fault_diagnosis"] = """
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
"""

BUILTIN_DOCS["oee_kpi_standards"] = """
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
"""

BUILTIN_DOCS["plc_opcua_integration"] = """
# PLC & OPC-UA Entegrasyon Rehberi

## OPC UA (Unified Architecture)

### Genel Bakış
OPC UA (IEC 62541), endüstriyel otomasyon için platform-bağımsız iletişim standardıdır.
MQTT ile birlikte Industry 4.0 iletişiminin temelini oluşturur.

### OPC UA vs MQTT
| Özellik | OPC UA | MQTT |
|---------|--------|------|
| Protokol | Client-Server | Publish-Subscribe |
| Güvenlik | Yerleşik (certificates) | TLS |
| Veri Modeli | Zengin (information model) | Payload-agnostic |
| Discovery | Yerleşik | Broker-based |
| Kullanım | Makine-to-MES | IoT/Edge-to-Cloud |
| Port | 4840 (default) | 1883/8883 |

### OPC UA over MQTT (Pub/Sub)
OPC UA Part 14: İki protokolün birleşimi.
- OPC UA veri modeli + MQTT transport
- Sparkplug B spesifikasyonu ile uyumlu

## Siemens S7 OPC UA Konfigürasyonu

### TIA Portal OPC UA Server
1. CPU özelliklerinde OPC UA Server aktifleştir
2. Server endpoint: opc.tcp://<ip>:4840
3. Security policy: Basic256Sha256 (önerilen)
4. Güvenlik modları: None, Sign, SignAndEncrypt

### Veri Etiketleri (Tag Mapping)
```
Namespace: urn:siemens:s7:
Node yapısı:
  Server → Objects → PLC → DataBlock → Tag

Örnek:
  ns=3;s="DB_Sensors"."Temperature"
  ns=3;s="DB_Sensors"."Vibration_RMS"
  ns=3;s="DB_Sensors"."Motor_Current"
```

### Data Quality Flags
OPC UA StatusCode değerleri:
| Code | Anlamı |
|------|--------|
| Good (0x00) | Değer güvenilir |
| Uncertain (0x40) | Değer şüpheli |
| Bad (0x80) | Değer güvenilmez |
| BadSensorFailure | Sensör arızası |
| BadDeviceFailure | Cihaz arızası |
| BadCommunication | İletişim hatası |

### Sampling ve Publishing
- Sampling Interval: Sensör okuma hızı (min 100ms)
- Publishing Interval: Client'a gönderme hızı (min 500ms)
- Deadband: Değişim eşiği (absolute veya percent)
  - Absolute: ±0.1 (değer bu kadar değişmezse göndermez)
  - Percent: ±1% (yüzde bazlı)

## Beckhoff TwinCAT OPC UA

### Konfigürasyon
- TwinCAT 3: OPC UA Server (TE1000)
- TcCOM: TMC/ADS tabanlı veri modeli
- Namespace: http://beckhoff.com/twincat/
- Default endpoint: opc.tcp://localhost:4840

### Data Access
```
Symbol Path: MAIN.fbMotor.fVibration
Access: Read/Write
Data Type: LREAL
```

## MQTT Topic Yapısı (Sparkplug B uyumlu)

### Topic Hierarchy
```
spBv1.0/<group_id>/DDATA/<edge_node_id>/<device_id>

Örnek:
  spBv1.0/Factory1/DDATA/Line1/Motor001
  spBv1.0/Factory1/DBIRTH/Line1/Motor001
  spBv1.0/Factory1/DDEATH/Line1/Motor001
```

### Payload (JSON)
```json
{
  "timestamp": 1700000000000,
  "metrics": [
    {"name": "Temperature", "value": 65.2, "type": "Double", "quality": "Good"},
    {"name": "Vibration_RMS", "value": 3.5, "type": "Double", "quality": "Good"},
    {"name": "Motor_Current", "value": 10.8, "type": "Double", "quality": "Good"},
    {"name": "Running", "value": true, "type": "Boolean"}
  ],
  "seq": 42
}
```
"""

BUILTIN_DOCS["alarm_management"] = """
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
"""

BUILTIN_DOCS["predictive_maintenance"] = """
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
  |   \
  |    \  ← P-F Interval (müdahale penceresi)
  |     \
  |      F (Functional Failure - Makine durur)
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
"""


class KnowledgeBase:
    """
    Hazır endüstriyel bilgi bankası
    Sistem başlatıldığında otomatik yüklenir
    """

    def __init__(
        self,
        vector_store: VectorStore,
        docs_directory: str = "./rag_docs",
        auto_load_builtin: bool = True
    ):
        self.vector_store = vector_store
        self.docs_directory = Path(docs_directory)
        self.docs_directory.mkdir(parents=True, exist_ok=True)
        self.processor = DocumentProcessor(chunk_size=500, chunk_overlap=50)
        self.loaded_docs: List[str] = []

        if auto_load_builtin:
            self.load_builtin_docs()

    def load_builtin_docs(self) -> int:
        """Yerleşik dokümanları yükle"""
        total_chunks = 0

        for doc_name, content in BUILTIN_DOCS.items():
            if doc_name in self.loaded_docs:
                continue

            chunks = self.processor.chunk_text(content, source=f"builtin:{doc_name}")

            doc = Document(
                id=self.processor.generate_id(content, doc_name),
                filename=f"{doc_name}.md",
                filepath=f"builtin:{doc_name}",
                doc_type="builtin",
                chunks=chunks,
                metadata={"type": "builtin", "name": doc_name}
            )

            added = self.vector_store.add_document(doc)
            total_chunks += added
            self.loaded_docs.append(doc_name)
            logger.info(f"Builtin yüklendi: {doc_name} ({added} chunk)")

        return total_chunks

    def load_user_document(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Kullanıcı dokümanı yükle"""
        doc = self.processor.process_file(filepath)
        if not doc:
            return None

        added = self.vector_store.add_document(doc)
        self.loaded_docs.append(doc.filename)

        return {
            "filename": doc.filename,
            "doc_type": doc.doc_type,
            "chunks": added,
            "categories": doc.metadata.get("categories", []),
            "pages": doc.metadata.get("total_pages", 0)
        }

    def load_user_directory(self, dirpath: str = None) -> List[Dict[str, Any]]:
        """Dizindeki tüm dokümanları yükle"""
        target = dirpath or str(self.docs_directory)
        documents = self.processor.process_directory(target)

        results = []
        for doc in documents:
            added = self.vector_store.add_document(doc)
            self.loaded_docs.append(doc.filename)
            results.append({
                "filename": doc.filename,
                "doc_type": doc.doc_type,
                "chunks": added,
                "categories": doc.metadata.get("categories", [])
            })

        return results

    def add_custom_knowledge(self, title: str, content: str, category: str = "general") -> int:
        """Özel bilgi ekle (metin olarak)"""
        chunks = self.processor.chunk_text(content, source=f"custom:{title}")

        doc = Document(
            id=self.processor.generate_id(content, title),
            filename=f"{title}.txt",
            filepath=f"custom:{title}",
            doc_type="custom",
            chunks=chunks,
            metadata={"type": "custom", "category": category}
        )

        added = self.vector_store.add_document(doc)
        self.loaded_docs.append(title)
        return added

    def get_status(self) -> Dict[str, Any]:
        """Bilgi bankası durumu"""
        store_stats = self.vector_store.get_stats()

        return {
            "loaded_documents": self.loaded_docs,
            "total_documents": len(self.loaded_docs),
            "builtin_docs": list(BUILTIN_DOCS.keys()),
            "docs_directory": str(self.docs_directory),
            "vector_store": store_stats
        }

    def save_builtin_as_files(self):
        """Yerleşik dokümanları dosya olarak kaydet (referans için)"""
        builtin_dir = self.docs_directory / "builtin"
        builtin_dir.mkdir(parents=True, exist_ok=True)

        for doc_name, content in BUILTIN_DOCS.items():
            filepath = builtin_dir / f"{doc_name}.md"
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)

        logger.info(f"Builtin dokümanlar kaydedildi: {builtin_dir}")
