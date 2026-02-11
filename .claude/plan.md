# SmartFact 10-Feature Implementation Plan

## Implementation Order (Sprint Bazli)

### Sprint 1: PackML State Machine (#2)
**Neden ilk:** Tüm diğer featurelar için temel. Makine durumlarını standartlaştırır.

**Yeni dosyalar:**
- `packml/__init__.py`
- `packml/state_machine.py` — PackMLState enum, VALID_TRANSITIONS, PackMLMachine sınıfı
- `packml/packml_api.py` — FastAPI router `/api/packml/*`

**Değişen dosyalar:**
- `config.py` — PackMLConfig dataclass
- `schema.py` — `packml_state_log` tablosu
- `ui.py` — Router mount, IoT modda "Durum Makinesi" sekmesi, SVG state diagram

**API:** GET /states, POST /command, GET /history/{machine}

---

### Sprint 2: OPC UA Gateway (#1)
**Yeni dosyalar:**
- `opcua_gateway.py` — OPCUAGateway (client) + OPCUAServer (sim) sınıfları
- `opcua_gateway_config.json`

**Değişen dosyalar:**
- `config.py` — OPCUAConfig
- `ui.py` — Kontrol Paneli'ne OPC UA başlat/durdur butonu

**Bağımlılık:** PackML node'larını OPC UA address space'e expose eder

---

### Sprint 3: MES Work Orders (#4) + Recipe Management (#5)
**Yeni dosyalar:**
- `mes/__init__.py`
- `mes/work_order.py` — WorkOrderService CRUD
- `mes/recipe.py` — RecipeService + versiyon kontrolü
- `mes/shift_report.py` — Vardiya raporu
- `mes/mes_api.py` — FastAPI router `/api/mes/*`

**DB tabloları:** `mes_work_order`, `mes_wo_event`, `recipe_master`, `recipe_parameter`, `recipe_audit_log`

**UI:** ERP modda "Üretim Emirleri" + "Reçete Yönetimi" sekmeleri

**Entegrasyon:** İş emri başlat → PackML Start, Reçete uygula → OPC UA write

---

### Sprint 4: Digital Twin (#3)
**Yeni dosyalar:**
- `digital_twin/__init__.py`
- `digital_twin/physics_model.py` — Fizik tabanlı beklenen değer hesabı
- `digital_twin/twin_engine.py` — Gerçek vs beklenen karşılaştırma
- `digital_twin/twin_api.py` — FastAPI router `/api/twin/*`

**DB:** `twin_deviation_log` tablosu

**UI:** IoT modda "Dijital İkiz" sekmesi — gauge'lar, sapma yüzdesi, sağlık skoru

---

### Sprint 5: SPC (#8) + Condition Monitoring (#9)
**Yeni dosyalar:**
- `spc/__init__.py`, `spc/control_chart.py`, `spc/capability.py`, `spc/nelson_rules.py`, `spc/spc_api.py`
- `condition_monitoring/__init__.py`, `condition_monitoring/fft_analysis.py`, `condition_monitoring/bearing_diagnosis.py`, `condition_monitoring/rul_estimator.py`, `condition_monitoring/cm_api.py`

**UI:** IoT modda "SPC" + "Durum İzleme" sekmeleri

---

### Sprint 6: Energy Monitoring (#6) + Traceability (#7)
**Yeni dosyalar:**
- `energy/__init__.py`, `energy/energy_service.py`, `energy/energy_api.py`
- `traceability/__init__.py`, `traceability/trace_service.py`, `traceability/trace_api.py`, `traceability/dmc_generator.py`

**DB:** `fact_energy_readings`, `trace_part`, `trace_part_params`, `trace_part_event`

**UI:** IoT "Enerji İzleme" + ERP "İzlenebilirlik" sekmeleri

---

### Sprint 7: Edge Computing (#10)
**Yeni dosyalar:**
- `edge/__init__.py`, `edge/store_forward.py`, `edge/edge_rules.py`, `edge/edge_api.py`

**Değişen:** `ingest_consumer.py` — DB fail durumunda SQLite buffer

**UI:** Kontrol Paneli'ne edge status, rule yönetimi eklenir

---

## Toplam: ~30 yeni dosya, 7 yeni paket
## Her feature: kendi modülü + FastAPI router + config dataclass + UI sekmesi
