import os
from typing import Any

import yaml


DEFAULT_CONFIG_CANDIDATES = (
    os.environ.get("PIPELINE_CONFIG"),
    "/data/config/pipeline_config.yaml",
    "/app/config/pipeline_config.yaml",
    "config/pipeline_config.yaml",
)

DQ_RULES_CANDIDATES = (
    os.environ.get("DQ_RULES_CONFIG"),
    "/data/config/dq_rules.yaml",
    "/app/config/dq_rules.yaml",
    "config/dq_rules.yaml",
)


def load_config() -> dict[str, Any]:
    for candidate in DEFAULT_CONFIG_CANDIDATES:
        if candidate and os.path.exists(candidate):
            with open(candidate, "r", encoding="utf-8") as handle:
                return yaml.safe_load(handle)

    raise FileNotFoundError("Could not locate pipeline_config.yaml in any expected location")


def load_dq_rules() -> dict[str, Any]:
    for candidate in DQ_RULES_CANDIDATES:
        if candidate and os.path.exists(candidate):
            with open(candidate, "r", encoding="utf-8") as handle:
                return yaml.safe_load(handle) or {}
    return {}
