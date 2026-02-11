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

-- ═══════════════════════════════════════════════════════════════════════
-- PackML State Machine
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS packml_state_log (
    id BIGSERIAL PRIMARY KEY,
    machine_id INT REFERENCES dim_machine(machine_id),
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    command TEXT,
    triggered_by TEXT,
    ts TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_packml_machine_ts ON packml_state_log(machine_id, ts DESC);

-- ═══════════════════════════════════════════════════════════════════════
-- MES Work Orders & Recipes
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS recipe_master (
    recipe_id SERIAL PRIMARY KEY,
    recipe_code TEXT UNIQUE NOT NULL,
    product_code TEXT NOT NULL,
    version INT NOT NULL DEFAULT 1,
    status TEXT DEFAULT 'draft',
    description TEXT,
    created_by TEXT DEFAULT 'system',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS recipe_parameter (
    id SERIAL PRIMARY KEY,
    recipe_id INT REFERENCES recipe_master(recipe_id),
    param_name TEXT NOT NULL,
    param_value REAL NOT NULL,
    unit TEXT,
    min_value REAL,
    max_value REAL
);

CREATE TABLE IF NOT EXISTS recipe_audit_log (
    id BIGSERIAL PRIMARY KEY,
    recipe_id INT REFERENCES recipe_master(recipe_id),
    action TEXT NOT NULL,
    old_values JSONB,
    new_values JSONB,
    changed_by TEXT DEFAULT 'system',
    ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mes_work_order (
    wo_id SERIAL PRIMARY KEY,
    wo_number TEXT UNIQUE NOT NULL,
    product_code TEXT NOT NULL,
    recipe_id INT REFERENCES recipe_master(recipe_id),
    machine_id INT REFERENCES dim_machine(machine_id),
    status TEXT DEFAULT 'planned',
    target_qty INT NOT NULL,
    actual_qty INT DEFAULT 0,
    scrap_qty INT DEFAULT 0,
    planned_start TIMESTAMP,
    planned_end TIMESTAMP,
    actual_start TIMESTAMP,
    actual_end TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mes_wo_event (
    id BIGSERIAL PRIMARY KEY,
    wo_id INT REFERENCES mes_work_order(wo_id),
    event_type TEXT NOT NULL,
    detail JSONB,
    operator TEXT DEFAULT 'system',
    ts TIMESTAMP DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════
-- Digital Twin Deviation Log
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS twin_deviation_log (
    id BIGSERIAL PRIMARY KEY,
    machine_id INT REFERENCES dim_machine(machine_id),
    sensor TEXT NOT NULL,
    expected_value REAL,
    actual_value REAL,
    deviation_pct REAL,
    severity TEXT,
    ts TIMESTAMP DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════
-- Energy Monitoring
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS fact_energy_readings (
    id BIGSERIAL PRIMARY KEY,
    machine_id INT REFERENCES dim_machine(machine_id),
    time_id INT REFERENCES dim_time(time_id),
    shift_id INT REFERENCES dim_shift(shift_id),
    power_kw REAL,
    power_factor REAL,
    energy_kwh REAL,
    voltage REAL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════
-- Traceability
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS trace_part (
    part_id BIGSERIAL PRIMARY KEY,
    dmc_code TEXT UNIQUE NOT NULL,
    wo_id INT REFERENCES mes_work_order(wo_id),
    machine_id INT REFERENCES dim_machine(machine_id),
    recipe_id INT,
    product_code TEXT NOT NULL,
    produced_at TIMESTAMP DEFAULT NOW(),
    quality_status TEXT DEFAULT 'ok',
    batch_number TEXT
);

CREATE TABLE IF NOT EXISTS trace_part_params (
    id BIGSERIAL PRIMARY KEY,
    part_id BIGINT REFERENCES trace_part(part_id),
    param_name TEXT NOT NULL,
    param_value REAL,
    unit TEXT,
    within_spec BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS trace_part_event (
    id BIGSERIAL PRIMARY KEY,
    part_id BIGINT REFERENCES trace_part(part_id),
    event_type TEXT NOT NULL,
    detail JSONB,
    ts TIMESTAMP DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════
-- Condition Monitoring
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS cm_vibration_spectrum (
    id BIGSERIAL PRIMARY KEY,
    machine_id INT REFERENCES dim_machine(machine_id),
    spectrum_data JSONB,
    rms_velocity REAL,
    peak_frequency REAL,
    iso10816_zone TEXT,
    ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cm_bearing_config (
    id SERIAL PRIMARY KEY,
    machine_id INT REFERENCES dim_machine(machine_id),
    bearing_model TEXT,
    n_balls INT,
    ball_diameter REAL,
    pitch_diameter REAL,
    contact_angle REAL,
    rpm REAL
);

CREATE TABLE IF NOT EXISTS cm_rul_history (
    id BIGSERIAL PRIMARY KEY,
    machine_id INT REFERENCES dim_machine(machine_id),
    component TEXT,
    rul_hours REAL,
    confidence REAL,
    ts TIMESTAMP DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════
-- Materialized Views
-- ═══════════════════════════════════════════════════════════════════════

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
