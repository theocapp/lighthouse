import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class DatabaseConfig:
    url: str = "postgresql+psycopg://localhost:5432/lighthouse"
    raw_schema: str = "raw"
    core_schema: str = "core"
    analytics_schema: str = "analytics"


@dataclass
class ApiKeys:
    congress_gov: str = ""
    fec: str = ""
    financial_modeling_prep: str = ""


@dataclass
class RateLimits:
    congress_api: float = 1.3
    fec_api: float = 0.003
    sec_edgar: float = 10.0
    house_watcher: float = 1.0
    senate_watcher: float = 1.0
    govinfo: float = 2.0


@dataclass
class DataConfig:
    cache_dir: str = "./data"
    output_dir: str = "./output"
    legislators_path: str = "./data/legislators-current.csv"
    billstatus_xml_dir: str = "./data/billstatus_xml"
    bill_cache_days: int = 1
    disclosure_cache_days: int = 7


@dataclass
class FecWarehouseConfig:
    source_db_url: str = ""
    raw_dir: str = "./data/fec"
    cycles: list[int] = field(default_factory=lambda: [2022, 2024, 2026])
    prefer_local_db: bool = True


@dataclass
class RuleWeights:
    vote_holding: float = 0.85
    trade_timing_pre: float = 1.00
    trade_timing_post: float = 0.70
    sponsorship_holding: float = 0.90
    committee_donor: float = 0.65
    family_holding: float = 0.75


@dataclass
class DetectionConfig:
    trade_window_days: int = 30
    min_holding_value: float = 1000.0
    mutual_fund_discount: float = 0.2
    family_holding_discount: float = 0.6
    rule_weights: RuleWeights = field(default_factory=RuleWeights)


@dataclass
class CongressConfig:
    current: int = 119
    chambers: list = field(default_factory=lambda: ["house", "senate"])


@dataclass
class Config:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    api_keys: ApiKeys = field(default_factory=ApiKeys)
    rate_limits: RateLimits = field(default_factory=RateLimits)
    data: DataConfig = field(default_factory=DataConfig)
    fec_warehouse: FecWarehouseConfig = field(default_factory=FecWarehouseConfig)
    detection: DetectionConfig = field(default_factory=DetectionConfig)
    congress: CongressConfig = field(default_factory=CongressConfig)

    @property
    def cache_dir(self) -> Path:
        return Path(self.data.cache_dir)

    @property
    def output_dir(self) -> Path:
        return Path(self.data.output_dir)


def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_config(path: Optional[str] = None) -> Config:
    raw: dict = {}

    config_path = path or os.environ.get("LIGHTHOUSE_CONFIG", "config.yml")
    if os.path.exists(config_path):
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

    # Environment variable overrides
    env_overrides: dict = {}
    if key := os.environ.get("CONGRESS_API_KEY"):
        env_overrides.setdefault("api_keys", {})["congress_gov"] = key
    if key := os.environ.get("FEC_API_KEY"):
        env_overrides.setdefault("api_keys", {})["fec"] = key
    if url := os.environ.get("DATABASE_URL"):
        env_overrides.setdefault("database", {})["url"] = url

    raw = _deep_merge(raw, env_overrides)

    db = DatabaseConfig(**raw.get("database", {}))
    keys_raw = raw.get("api_keys", {})
    api_keys = ApiKeys(**{k: v for k, v in keys_raw.items() if hasattr(ApiKeys, k)})
    rl_raw = raw.get("rate_limits", {})
    rate_limits = RateLimits(**{k: v for k, v in rl_raw.items() if hasattr(RateLimits, k)})
    data_raw = raw.get("data", {})
    data = DataConfig(**{k: v for k, v in data_raw.items() if hasattr(DataConfig, k)})
    fec_raw = raw.get("fec_warehouse", {})
    fec_warehouse = FecWarehouseConfig(
        **{k: v for k, v in fec_raw.items() if hasattr(FecWarehouseConfig, k)}
    )

    det_raw = raw.get("detection", {})
    weights_raw = det_raw.pop("rule_weights", {})
    weights = RuleWeights(**{k: v for k, v in weights_raw.items() if hasattr(RuleWeights, k)})
    detection = DetectionConfig(
        rule_weights=weights,
        **{k: v for k, v in det_raw.items() if hasattr(DetectionConfig, k) and k != "rule_weights"},
    )

    congress_raw = raw.get("congress", {})
    congress = CongressConfig(**{k: v for k, v in congress_raw.items() if hasattr(CongressConfig, k)})

    config = Config(
        database=db,
        api_keys=api_keys,
        rate_limits=rate_limits,
        data=data,
        fec_warehouse=fec_warehouse,
        detection=detection,
        congress=congress,
    )

    os.environ["LIGHTHOUSE_DB_RAW_SCHEMA"] = config.database.raw_schema
    os.environ["LIGHTHOUSE_DB_CORE_SCHEMA"] = config.database.core_schema
    os.environ["LIGHTHOUSE_DB_ANALYTICS_SCHEMA"] = config.database.analytics_schema

    return config


# Module-level singleton — callers do: from lighthouse.config import config
config = load_config()
