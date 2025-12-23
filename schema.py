"""DDL for star schema and KPI views."""

DDL = """
CREATE TABLE IF NOT EXISTS dim_machine (
    machine_id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    line TEXT NOT NULL,
    model TEXT,
    install_date DATE
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL UNIQUE,
    date DATE NOT NULL,
    hour INT NOT NULL,
    weekday INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_shift (
    shift_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    start_hour INT NOT NULL,
    end_hour INT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sensor_readings (
    id BIGSERIAL PRIMARY KEY,
    machine_id INT REFERENCES dim_machine(machine_id),
    time_id INT REFERENCES dim_time(time_id),
    shift_id INT REFERENCES dim_shift(shift_id),
    temperature REAL,
    vibration REAL,
    current REAL,
    throughput INT,
    failure_flag BOOLEAN,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kpi_oee AS
SELECT
    m.line,
    m.code,
    date(t.ts) AS day,
    avg(CASE WHEN f.failure_flag THEN 0 ELSE 1 END)::float AS availability,
    avg(f.throughput)::float AS performance,
    1.0 AS quality,
    avg(CASE WHEN f.failure_flag THEN 0 ELSE 1 END)::float
      * avg(f.throughput)::float
      * 1.0 AS oee
FROM fact_sensor_readings f
JOIN dim_machine m ON f.machine_id = m.machine_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY m.line, m.code, date(t.ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_mttr AS
SELECT m.code, avg(mttr_minutes) AS mttr_minutes
FROM (
    SELECT machine_id, (lead(t.ts) OVER w - t.ts) / interval '1 minute' AS mttr_minutes
    FROM fact_sensor_readings f
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE failure_flag = true
    WINDOW w AS (PARTITION BY machine_id ORDER BY t.ts)
) s
JOIN dim_machine m ON s.machine_id = m.machine_id
GROUP BY m.code;
"""
