"""Central configuration for Smart Factory Simulator."""
from dataclasses import dataclass
import os


def _get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value


@dataclass
class MQTTConfig:
    host: str = os.getenv("MQTT_HOST", "localhost")
    port: int = int(os.getenv("MQTT_PORT", "1883"))
    username: str | None = os.getenv("MQTT_USERNAME")
    password: str | None = os.getenv("MQTT_PASSWORD")
    base_topic: str = os.getenv("MQTT_BASE_TOPIC", "factory")
    qos: int = int(os.getenv("MQTT_QOS", "1"))


@dataclass
class DBConfig:
    uri: str = _get_env("DATABASE_URL", "postgresql://factory:factory@localhost:5432/factory")
    schema: str = os.getenv("DB_SCHEMA", "public")


@dataclass
class LocaleConfig:
    supported: tuple[str, ...] = ("tr", "de", "en")
    default: str = os.getenv("DEFAULT_LOCALE", "tr")


@dataclass
class ModelConfig:
    sequence_length: int = int(os.getenv("MODEL_SEQ_LEN", "50"))
    batch_size: int = int(os.getenv("MODEL_BATCH_SIZE", "64"))
    hidden_size: int = int(os.getenv("MODEL_HIDDEN_SIZE", "64"))
    learning_rate: float = float(os.getenv("MODEL_LR", "1e-3"))
    num_epochs: int = int(os.getenv("MODEL_EPOCHS", "3"))
    checkpoint_path: str = os.getenv("MODEL_CKPT_PATH", "artifacts/lstm.ckpt")


@dataclass
class APIConfig:
    host: str = os.getenv("API_HOST", "0.0.0.0")
    port: int = int(os.getenv("API_PORT", "8000"))
    debug: bool = os.getenv("API_DEBUG", "true").lower() == "true"


@dataclass
class RAGConfig:
    data_dir: str = os.getenv("RAG_DATA_DIR", "./rag_data")
    docs_dir: str = os.getenv("RAG_DOCS_DIR", "./rag_docs")
    embedding_model: str = os.getenv("RAG_EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    llm_provider: str = os.getenv("RAG_LLM_PROVIDER", "ollama")
    model_name: str = os.getenv("RAG_MODEL_NAME", "llama2")
    api_base: str = os.getenv("RAG_API_BASE", "http://localhost:11434")
    api_key: str | None = os.getenv("RAG_API_KEY")


@dataclass
class ERPConfig:
    data_dir: str = os.getenv("ERP_DATA_DIR", "./erp_mes/data")
    model_path: str = os.getenv("ERP_MODEL_PATH", "./erp_mes/models")


@dataclass
class PackMLConfig:
    auto_transition_enabled: bool = os.getenv("PACKML_AUTO_TRANSITION", "true").lower() == "true"
    default_machines: str = os.getenv("PACKML_MACHINES", "MX100,MX200")
    alarm_auto_hold: bool = os.getenv("PACKML_ALARM_AUTO_HOLD", "true").lower() == "true"


@dataclass
class OPCUAConfig:
    server_url: str = os.getenv("OPCUA_SERVER_URL", "opc.tcp://localhost:4840")
    namespace: str = os.getenv("OPCUA_NAMESPACE", "urn:smartfact:server")
    security_policy: str = os.getenv("OPCUA_SECURITY", "None")
    username: str | None = os.getenv("OPCUA_USERNAME")
    password: str | None = os.getenv("OPCUA_PASSWORD")
    subscription_interval: int = int(os.getenv("OPCUA_SUB_INTERVAL_MS", "500"))
    simulation_mode: bool = os.getenv("OPCUA_SIMULATION", "true").lower() == "true"


@dataclass
class DigitalTwinConfig:
    deviation_warning_pct: float = float(os.getenv("TWIN_WARNING_PCT", "10"))
    deviation_anomaly_pct: float = float(os.getenv("TWIN_ANOMALY_PCT", "15"))
    history_size: int = int(os.getenv("TWIN_HISTORY_SIZE", "200"))
    update_interval: float = float(os.getenv("TWIN_UPDATE_INTERVAL", "1.0"))


@dataclass
class MESConfig:
    auto_simulate: bool = os.getenv("MES_AUTO_SIMULATE", "true").lower() == "true"
    default_shift_hours: int = int(os.getenv("MES_SHIFT_HOURS", "8"))
    scrap_threshold_pct: float = float(os.getenv("MES_SCRAP_THRESHOLD", "5.0"))


@dataclass
class EnergyConfig:
    nominal_voltage: float = float(os.getenv("ENERGY_VOLTAGE", "400"))
    target_kwh_per_unit: float = float(os.getenv("ENERGY_TARGET_KWH", "0.5"))
    co2_factor: float = float(os.getenv("ENERGY_CO2_FACTOR", "0.4"))


@dataclass
class SPCConfig:
    subgroup_size: int = int(os.getenv("SPC_SUBGROUP_SIZE", "5"))
    history_subgroups: int = int(os.getenv("SPC_HISTORY", "50"))
    nelson_rules_enabled: bool = os.getenv("SPC_NELSON_RULES", "true").lower() == "true"


@dataclass
class ConditionMonitoringConfig:
    sample_rate: int = int(os.getenv("CM_SAMPLE_RATE", "1000"))
    fft_size: int = int(os.getenv("CM_FFT_SIZE", "1024"))
    rul_model: str = os.getenv("CM_RUL_MODEL", "exponential")


@dataclass
class TraceConfig:
    dmc_prefix: str = os.getenv("TRACE_DMC_PREFIX", "SF")
    batch_auto_create: bool = os.getenv("TRACE_AUTO_BATCH", "true").lower() == "true"


@dataclass
class EdgeConfig:
    mode: str = os.getenv("EDGE_MODE", "standalone")
    local_buffer_path: str = os.getenv("EDGE_BUFFER_PATH", "./edge_buffer.db")
    cloud_mqtt_host: str = os.getenv("CLOUD_MQTT_HOST", "")
    cloud_mqtt_port: int = int(os.getenv("CLOUD_MQTT_PORT", "8883"))
    sync_interval: int = int(os.getenv("EDGE_SYNC_INTERVAL", "30"))
    max_buffer_size: int = int(os.getenv("EDGE_MAX_BUFFER", "100000"))
    rule_check_interval_ms: int = int(os.getenv("EDGE_RULE_INTERVAL_MS", "10"))


mqtt = MQTTConfig()
db = DBConfig()
locales = LocaleConfig()
model_cfg = ModelConfig()
api_cfg = APIConfig()
rag_cfg = RAGConfig()
erp_cfg = ERPConfig()
packml_cfg = PackMLConfig()
opcua_cfg = OPCUAConfig()
twin_cfg = DigitalTwinConfig()
mes_cfg = MESConfig()
energy_cfg = EnergyConfig()
spc_cfg = SPCConfig()
cm_cfg = ConditionMonitoringConfig()
trace_cfg = TraceConfig()
edge_cfg = EdgeConfig()
