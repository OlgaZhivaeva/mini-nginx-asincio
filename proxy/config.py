import yaml
from pydantic import BaseModel


class UpstreamConfig(BaseModel):
    host: str
    port: int


class TimeoutConfig(BaseModel):
    connect_ms: int
    read_ms: int
    write_ms: int
    total_ms: int


class LimitConfig(BaseModel):
    max_client_conns: int
    max_conns_per_upstream: int


class LoggingConfig(BaseModel):
    level: str = "info"


class AppConfig(BaseModel):
    listen: str
    upstreams: list[UpstreamConfig]
    timeouts: TimeoutConfig
    limits: LimitConfig
    logging: LoggingConfig


def load_config(config_path: str = "config.yaml") -> AppConfig:
    """Читает YAML файл и валидирует его через Pydantic."""
    with open(config_path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f)

    return AppConfig.model_validate(raw_data)
