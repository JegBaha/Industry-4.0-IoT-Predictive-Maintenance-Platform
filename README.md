🇹🇷
Akıllı Fabrika (Smart Factory) Simülasyonu – IIoT & AI PoC
MQTT tabanlı gerçek zamanlı sensör akışı ile çalışan, üretim hatları için KPI (OEE, MTTR), alarm yönetimi ve arıza olasılığı tahmini sunan uçtan uca bir akıllı fabrika simülasyonu geliştirdim. Sistem; sentetik sensör verisi üretimi, Postgres tabanlı veri katmanı, FastAPI ile API + canlı dashboard ve zaman serisi tabanlı ML bileşenlerini içermektedir.
Teknolojiler: Python, FastAPI, MQTT (Mosquitto), PostgreSQL, LSTM, Time-Series ML, HTML/CSS/JS.

Sınırlamalar / Notlar:

Arıza risk skoru UI tarafında demo amaçlı üretiliyor (ML API ile tam entegre değil).

Frontend ayrı bir uygulama değil, FastAPI içinde gömülü tek sayfa olarak çalışıyor.

Docker / Power BI entegrasyonu README’de yer alsa da repoda aktif değil.

🇬🇧
Developed an end-to-end smart factory simulation leveraging MQTT-based real-time sensor streaming to deliver production KPIs (OEE, MTTR), alarm management, and failure probability estimation. The system includes synthetic sensor data generation, a PostgreSQL-based data layer, FastAPI serving both APIs and a live dashboard, and time-series machine learning components.
Technologies: Python, FastAPI, MQTT (Mosquitto), PostgreSQL, LSTM, Time-Series ML, HTML/CSS/JavaScript.

Limitations / Notes:

Failure risk score is currently generated on the UI side for demo purposes and is not fully integrated with the ML inference API.

Frontend is embedded within FastAPI as a single-page UI rather than a standalone web application.

Docker Compose and Power BI integration are mentioned in the README but not fully implemented in the repository.
<img width="1890" height="637" alt="Desktop 2025-12-26 10-23-00 PM-451" src="https://github.com/user-attachments/assets/998a98a6-5d62-4e40-951d-cdeb231db2a0" />
