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

<img width="1903" height="929" alt="Desktop 2026-01-28 11-26-03 PM-406" src="https://github.com/user-attachments/assets/0e243c29-26cc-4f4d-9969-ad4105c56538" />
<img width="1903" height="929" alt="Desktop 2026-01-28 11-26-03 PM-406 - Kopya" src="https://github.com/user-attachments/assets/cee38365-835b-45c3-a1b0-9d55353e2a8d" />
<img width="1903" height="929" alt="Desktop 2026-01-28 11-25-58 PM-649" src="https://github.com/user-attachments/assets/14a2dd9c-97ad-4ab4-b204-cb92cb741623" />
<img width="1903" height="523" alt="Desktop 2026-01-28 11-25-51 PM-368" src="https://github.com/user-attachments/assets/613cd71a-b2ce-4199-83d0-2263cdcfac14" />
<img width="1903" height="917" alt="Desktop 2026-01-28 11-25-42 PM-970" src="https://github.com/user-attachments/assets/f3d053ca-754a-4cb8-8b2b-8b303434b875" />
<img width="1903" height="917" alt="Desktop 2026-01-28 11-25-38 PM-910" src="https://github.com/user-attachments/assets/8ea0d6c6-cfd0-4eda-9940-c5386778a212" />
