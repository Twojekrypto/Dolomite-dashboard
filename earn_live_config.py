#!/usr/bin/env python3
"""
Shared live-audit configuration loader.

Operational knobs live here so we can tune localhost/debug URLs, worker counts,
and browser timing without editing audit logic.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = ROOT / "config" / "earn_live_defaults.json"

DEFAULT_CONFIG: Dict[str, Any] = {
    "auditEarnAsset": {
        "liveDefaults": {
            "localhostUrl": "http://127.0.0.1:8902/index.html?cb=earn_audit",
            "debugJsonUrl": "http://127.0.0.1:9555/json",
            "workers": 6,
        },
        "liveJs": {
            "pageTargetPollMs": 250,
            "pageReadyPollMs": 100,
            "pageReadyMaxWaitMs": 3000,
            "settlePollMs": 350,
            "settleStablePolls": 3,
            "timeoutFinalSnapshotDelayMs": 600,
            "snapshotFlushEveryResults": 12,
            "snapshotFlushMaxDelayMs": 2000,
        },
    },
    "runEarnChainLiveRerun": {
        "localhostUrl": "http://127.0.0.1:8921/index.html?cb=earn_chain_live_rerun",
        "debugJsonUrl": "http://127.0.0.1:9555/json",
        "workersPerMarket": 6,
        "retryWorkersPerMarket": 8,
        "maxMarkets": 5,
        "minUnresolvedCount": 1,
    },
    "presets": {
        "single-fast": {
            "auditEarnAsset": {
                "liveDefaults": {
                    "workers": 6,
                },
            },
            "runEarnChainLiveRerun": {
                "workersPerMarket": 6,
                "retryWorkersPerMarket": 8,
            },
        },
        "dual-sharded": {
            "auditEarnAsset": {
                "liveDefaults": {
                    "localhostUrl": (
                        "http://127.0.0.1:8921/index.html?cb=earn_audit_dual_a,"
                        "http://127.0.0.1:8921/index.html?cb=earn_audit_dual_b"
                    ),
                    "debugJsonUrl": "http://127.0.0.1:9555/json,http://127.0.0.1:9666/json",
                    "workers": 12,
                },
            },
            "runEarnChainLiveRerun": {
                "localhostUrl": (
                    "http://127.0.0.1:8921/index.html?cb=earn_chain_live_dual_a,"
                    "http://127.0.0.1:8921/index.html?cb=earn_chain_live_dual_b"
                ),
                "debugJsonUrl": "http://127.0.0.1:9555/json,http://127.0.0.1:9666/json",
                "workersPerMarket": 12,
                "retryWorkersPerMarket": 12,
            },
        },
    },
}


def parse_endpoint_values(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for item in value:
            out.extend(parse_endpoint_values(item))
        return out

    text = str(value).strip()
    if not text:
        return []

    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return parse_endpoint_values(parsed)

    return [item.strip() for item in text.split(",") if str(item).strip()]


def build_endpoint_pairs(localhost_value: Any, debug_value: Any) -> List[Dict[str, str]]:
    localhost_urls = parse_endpoint_values(localhost_value)
    debug_urls = parse_endpoint_values(debug_value)
    if not localhost_urls:
        raise ValueError("At least one localhost URL is required for live audit endpoints.")
    if not debug_urls:
        raise ValueError("At least one Chrome debug /json URL is required for live audit endpoints.")

    target_count = max(len(localhost_urls), len(debug_urls))
    if len(localhost_urls) not in {1, target_count}:
        raise ValueError(
            f"localhost endpoint count must be 1 or match debug endpoint count; got {len(localhost_urls)} vs {len(debug_urls)}"
        )
    if len(debug_urls) not in {1, target_count}:
        raise ValueError(
            f"debug endpoint count must be 1 or match localhost endpoint count; got {len(debug_urls)} vs {len(localhost_urls)}"
        )

    pairs: List[Dict[str, str]] = []
    for index in range(target_count):
        pairs.append(
            {
                "localhostUrl": localhost_urls[0 if len(localhost_urls) == 1 else index],
                "debugJsonUrl": debug_urls[0 if len(debug_urls) == 1 else index],
            }
        )
    return pairs


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _coerce_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        return int(default)
    return parsed if parsed > 0 else int(default)


def _normalize_audit_section(payload: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    section = _deep_merge(defaults, payload or {})
    live_defaults = section["liveDefaults"]
    live_defaults["localhostUrl"] = str(live_defaults.get("localhostUrl") or defaults["liveDefaults"]["localhostUrl"])
    live_defaults["debugJsonUrl"] = str(live_defaults.get("debugJsonUrl") or defaults["liveDefaults"]["debugJsonUrl"])
    live_defaults["workers"] = _coerce_positive_int(
        live_defaults.get("workers"),
        defaults["liveDefaults"]["workers"],
    )

    live_js = section["liveJs"]
    for key, default_value in defaults["liveJs"].items():
        live_js[key] = _coerce_positive_int(live_js.get(key), int(default_value))
    return section


def _normalize_rerun_section(payload: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    section = _deep_merge(defaults, payload or {})
    section["localhostUrl"] = str(section.get("localhostUrl") or defaults["localhostUrl"])
    section["debugJsonUrl"] = str(section.get("debugJsonUrl") or defaults["debugJsonUrl"])
    section["workersPerMarket"] = _coerce_positive_int(
        section.get("workersPerMarket"),
        defaults["workersPerMarket"],
    )
    section["retryWorkersPerMarket"] = _coerce_positive_int(
        section.get("retryWorkersPerMarket"),
        defaults["retryWorkersPerMarket"],
    )
    section["maxMarkets"] = _coerce_positive_int(
        section.get("maxMarkets"),
        defaults["maxMarkets"],
    )
    section["minUnresolvedCount"] = _coerce_positive_int(
        section.get("minUnresolvedCount"),
        defaults["minUnresolvedCount"],
    )
    return section


def _normalize_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    config = _deep_merge(DEFAULT_CONFIG, payload or {})
    config["auditEarnAsset"] = _normalize_audit_section(
        config.get("auditEarnAsset") or {},
        DEFAULT_CONFIG["auditEarnAsset"],
    )
    config["runEarnChainLiveRerun"] = _normalize_rerun_section(
        config.get("runEarnChainLiveRerun") or {},
        DEFAULT_CONFIG["runEarnChainLiveRerun"],
    )

    raw_presets = config.get("presets") or {}
    normalized_presets: Dict[str, Any] = {}
    for name, preset_payload in raw_presets.items():
        if not isinstance(name, str) or not isinstance(preset_payload, dict):
            continue
        normalized_presets[name] = {
            "auditEarnAsset": _normalize_audit_section(
                preset_payload.get("auditEarnAsset") or {},
                DEFAULT_CONFIG["auditEarnAsset"],
            ),
            "runEarnChainLiveRerun": _normalize_rerun_section(
                preset_payload.get("runEarnChainLiveRerun") or {},
                DEFAULT_CONFIG["runEarnChainLiveRerun"],
            ),
        }
    config["presets"] = normalized_presets
    return config


def get_config_path() -> Path:
    raw = os.environ.get("EARN_LIVE_CONFIG_PATH", "").strip()
    return Path(raw) if raw else DEFAULT_CONFIG_PATH


def load_earn_live_config() -> Dict[str, Any]:
    path = get_config_path()
    if not path.exists():
        return copy.deepcopy(DEFAULT_CONFIG)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"EARN live config must be a JSON object: {path}")
    return _normalize_config(payload)


def get_live_preset_names() -> List[str]:
    return sorted(str(name) for name in (load_earn_live_config().get("presets") or {}).keys())


def _resolve_section_defaults(section_key: str, preset_name: str | None) -> Dict[str, Any]:
    config = load_earn_live_config()
    section = copy.deepcopy(config[section_key])
    if not preset_name:
        return section
    presets = config.get("presets") or {}
    preset = presets.get(str(preset_name))
    if not isinstance(preset, dict):
        available = ", ".join(sorted(str(name) for name in presets.keys())) or "none"
        raise ValueError(f"Unknown EARN live preset: {preset_name}. Available presets: {available}")
    return _deep_merge(section, preset.get(section_key) or {})


def get_audit_earn_asset_defaults(preset_name: str | None = None) -> Dict[str, Any]:
    return _resolve_section_defaults("auditEarnAsset", preset_name)


def get_chain_live_rerun_defaults(preset_name: str | None = None) -> Dict[str, Any]:
    return _resolve_section_defaults("runEarnChainLiveRerun", preset_name)
