
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
