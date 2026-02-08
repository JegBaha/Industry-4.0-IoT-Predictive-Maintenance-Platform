Bu platform iki ana modülden oluşuyor. Birlikte uçtan uca bir Endüstri 4.0 çözümü oluşturuyorlar.

1. IoT İzleme Modülü (SmartFact)
Ne yapıyor: Fabrikanın fiziksel makinelerini (MX100, MX200) sensörler aracılığıyla canlı olarak izliyor, anomali algılıyor ve arıza tahmini yapıyor.

Veri Akışı

Sensörler (Simüle)  →  MQTT Broker  →  Dashboard (Canlı)
        ↓                                    ↓
   publish_stream()              Listener Thread (on_message)
   Her 2 sn'de bir               JSON parse → machine_states
   sıcaklık, titreşim,           → Alarm kontrolü
   akım, throughput               → Grafik güncelleme
        ↓
   Consumer Thread
   → PostgreSQL'e yaz
   → OEE/MTTR hesapla
Nasıl Çalışıyor (Adım Adım)
Simülasyon (ui.py:112-127): publish_stream() fonksiyonu her 2 saniyede MX100 ve MX200 için rastgele ama gerçekçi sensör verisi üretir (sıcaklık ~55°C, titreşim ~0.3 mm/s vs.) ve MQTT broker'a publish eder.

MQTT Broker (mosquitto_runner.py): Mosquitto broker mesajları alır ve subscriber'lara dağıtır. Topic yapısı: factory/MX100/sensors, factory/MX200/sensors.

Listener Thread (ui.py:146-186): MQTT'ye subscribe olup gelen her mesajı:

messages_buffer'a ekler (son 50 mesaj)
JSON parse edip machine_states[MX100].latest olarak saklar
history deque'sine ekler (son 200 okuma)
Alarm kontrolü yapar
Alarm Sistemi (ui.py:53-109): Her sensör okumasında eşik kontrolü yapar:

Sıcaklık: 60°C → warning, 70°C → critical
Titreşim: 0.5 mm/s → warning, 0.7 → critical
Akım: 12A → warning, 15A → critical
Throughput: 70% altı → warning, 50% altı → critical
Consumer Thread (ingest_consumer.py): MQTT mesajlarını alıp PostgreSQL veritabanına yazar. Star schema: fact_readings + dim_machine + dim_time.

KPI Hesaplama (db.py):

OEE = Availability × Performance × Quality (her makine için)
MTTR = Toplam tamir süresi / Arıza sayısı
Arıza Tahmini (api.py): PyTorch LSTM modeli geçmiş sensör verisinden gelecekteki arıza olasılığını tahmin eder.

RAG Asistan (rag/): ISO 10816, OEE standartları gibi teknik dokümanlarla soru-cevap. ChromaDB vektör veritabanı + LLM entegrasyonu.

Dashboard (IoT Modu) - 5 Tab
Tab	Ne Gösterir
Dashboard	Makine kartları (canlı sensör barları + arıza risk göstergesi), 4 KPI kartı, son alarmlar, sıcaklık trend grafiği
Makineler	Detaylı makine kartları + tüm sensörlerin karşılaştırma grafiği
Kontrol Paneli	Sistem başlat/durdur, thread durumları, canlı MQTT mesaj akışı
Alarmlar	Tüm alarm geçmişi, onaylama (acknowledge) butonu
RAG Asistan	Doküman tabanlı soru-cevap, sensör değer analizi, doküman yükleme
2. ERP/MES Modülü (AIProduction → erp_mes/)
Ne yapıyor: Üretim planlama (ERP) ve üretim yürütme (MES) verilerini birleştirip üretim performansını analiz ediyor ve makine öğrenmesiyle hata tahmini yapıyor.

Veri Akışı

ERP Verileri (erp.csv)     MES Verileri (mes.csv)
- Sipariş no               - Sipariş no
- Planlanan miktar          - Üretilen miktar
- Planlanan başlangıç/bitiş - Gerçek başlangıç/bitiş
                             - Hata miktarı
        ↓                          ↓
        └──── order_id ile merge ───┘
                    ↓
            Unified Table (120 kayıt)
                    ↓
        ┌───────────┼───────────┐
        ↓           ↓           ↓
   KPI Hesapla   Sipariş     ML Tahmin
                 Tablosu     (RandomForest)
Nasıl Çalışıyor (Adım Adım)
Veri Yükleme (erp_mes/data_service.py):

mes.csv: 120 üretim kaydı (sipariş no, üretilen miktar, hata miktarı, gerçek başlangıç/bitiş)
erp.csv: 120 planlama kaydı (sipariş no, planlanan miktar, planlanan başlangıç/bitiş)
order_id üzerinden inner merge yapılır → unified table
Bellekte cache'lenir (bir kez yüklenir)
KPI Hesaplama (erp_mes/kpi_service.py):

Plan Gerçekleşme = Üretilen / Planlanan (şu an %96.5 → hedef %95+)
Ortalama Gecikme = (Gerçek bitiş - Planlanan bitiş) saat cinsinden (şu an 1.02 saat → hedef ≤2)
Hurda Oranı = Hata miktarı / Üretilen (şu an %2.09 → hedef ≤%2)
Günlük trend hesaplama (41 günlük veri)
Hata Tahmini (erp_mes/ml_service.py):

Model: RandomForest Classifier (300 ağaç, max derinlik 12)
Girdiler: Sıcaklık, hat hızı, vardiya, operatör deneyimi, makine yaşı
Çıktı: Hata olasılığı (0-100%), güven skoru, risk seviyesi
sklearn modeli yoksa akıllı mock tahmin kullanır (fizik tabanlı formül)
Feature Importance: Hangi faktör ne kadar etkili (sıcaklık %35, hat hızı %25...)
Sıcaklık Eğrisi: 60-110°C arası sıcaklığın hata olasılığına etkisi
Analitik (erp_mes/ml_service.py:101-128):

95°C üstü sıcaklık → hata oranı önemli ölçüde artar
Gece vardiyası → %15 daha yüksek hata
Yüksek hız + yüksek sıcaklık = en riskli senaryo
5 yıldan az deneyimli operatörlerde hata artışı
Dashboard (ERP Modu) - 4 Tab
Tab	Ne Gösterir
KPI Dashboard	4 KPI kartı (plan gerçekleşme, gecikme, hurda, toplam sipariş) + feature importance bar chart + sıcaklık eğrisi grafiği
Siparişler	120 birleşik siparişin tablosu (renk kodlu: yeşil=iyi, sarı=uyarı, kırmızı=kötü)
Hata Tahmini	5 parametre girişi (sıcaklık, hız, vardiya, deneyim, makine yaşı) → olasılık + güven + risk + grafikler
Analitik	Model bilgisi, ML bulguları (4 ana tespit), aksiyonel öneriler listesi
İki Modülün Farkı
Özellik	IoT İzleme	ERP/MES
Veri kaynağı	MQTT sensörler (canlı)	CSV dosyaları (statik)
Güncelleme	Her 2 saniye	Sayfa yüklendiğinde
ML Modeli	LSTM (zaman serisi)	RandomForest (sınıflandırma)
Amaç	Makine sağlığı izleme	Üretim performans analizi
Alarm	Var (eşik tabanlı)	Yok
Veritabanı	PostgreSQL	Bellek içi cache
Odak	"Makine şu an nasıl?"	"Üretim planı tuttu mu?"
Teknik Mimari

                    ┌─────────────────────────┐
                    │    Tarayıcı (SPA)        │
                    │  HTML/CSS/JS embedded    │
                    │  IIFE → SmartFactory     │
                    │                          │
                    │  IoT Modu ←→ ERP Modu   │
                    │  (mode switcher)         │
                    └──────────┬──────────────┘
                               │ fetch()
                    ┌──────────▼──────────────┐
                    │    FastAPI (ui.py)        │
                    │    Port 8000              │
                    ├──────────────────────────┤
                    │ /api/machines   (IoT)     │
                    │ /api/kpis       (IoT)     │
                    │ /api/alarms     (IoT)     │
                    │ /api/rag/*      (RAG)     │
                    │ /api/erp/*      (ERP)     │
                    ├──────────┬───────────────┤
                    │          │               │
              ┌─────▼─────┐ ┌─▼────────────┐ ┌▼──────────┐
              │ MQTT +     │ │ erp_mes/     │ │ rag/      │
              │ PostgreSQL │ │ pandas +     │ │ ChromaDB +│
              │ + PyTorch  │ │ sklearn      │ │ LLM       │
              └────────────┘ └──────────────┘ └───────────┘
Kısacası: IoT modülü "fabrikada şu an ne oluyor?" sorusuna cevap verirken, ERP/MES modülü "üretim planımız ne kadar başarılı ve hatalar neden oluyor?" sorusuna cevap veriyor.

<img width="1917" height="922" alt="Desktop 2026-02-06 8-36-38 PM-330" src="https://github.com/user-attachments/assets/d4e3cefb-4264-4e67-955b-6a1db6bed56f" />
<img width="1906" height="922" alt="Desktop 2026-02-06 11-32-32 PM-39" src="https://github.com/user-attachments/assets/8985a2cb-e5ef-4a8c-90fa-c7d8612cd55c" />
<img width="1906" height="922" alt="Desktop 2026-02-06 11-32-35 PM-938" src="https://github.com/user-attachments/assets/af66c001-a16c-4b23-aace-5e1b49061152" />
<img width="1906" height="922" alt="Desktop 2026-02-06 11-32-39 PM-483" src="https://github.com/user-attachments/assets/76330279-ae7b-4a05-a381-76b3c317c3be" />
<img width="1906" height="922" alt="Desktop 2026-02-06 11-32-46 PM-711" src="https://github.com/user-attachments/assets/8ebc4d89-06c4-4a04-878d-1b4549e153d9" />
<img width="1906" height="922" alt="Desktop 2026-02-06 11-32-49 PM-944" src="https://github.com/user-attachments/assets/c62dc1d0-4d31-46fb-b128-c91b979dfa4d" />
<img width="1906" height="922" alt="Desktop 2026-02-06 11-32-53 PM-624" src="https://github.com/user-attachments/assets/8087086e-b99b-44b2-9be6-3d64a88e7d48" />
<img width="1906" height="922" alt="Desktop 2026-02-06 11-33-01 PM-357" src="https://github.com/user-attachments/assets/979c1322-84ea-4ad0-8d6a-c703db9cf062" />
<img width="1906" height="922" alt="Desktop 2026-02-06 11-33-04 PM-600" src="https://github.com/user-attachments/assets/43f12e14-94fc-41dc-ae01-8fb32b63cc0c" />

