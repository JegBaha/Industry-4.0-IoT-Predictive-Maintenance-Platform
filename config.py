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


mqtt = MQTTConfig()
db = DBConfig()
locales = LocaleConfig()
model_cfg = ModelConfig()
api_cfg = APIConfig()
