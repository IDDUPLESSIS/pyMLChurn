from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from .config import Config
from .db import execute_stored_procedure


STATE_DIR = Path('.state')
STATE_FILE = STATE_DIR / 'sp_runs.json'


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _save_state(state: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True), encoding='utf-8')


def _sp_key(cfg: Config, sp_name: str, schema: str) -> str:
    return "|".join([
        cfg.server.lower(),
        cfg.database.lower(),
        schema.lower(),
        sp_name.lower(),
    ])


@dataclass
class SPRunPolicy:
    ttl_hours: int = 24

    def ttl(self) -> timedelta:
        return timedelta(hours=self.ttl_hours)


def should_run(cfg: Config, sp_name: str, schema: str, policy: SPRunPolicy) -> tuple[bool, Optional[datetime]]:
    state = _load_state()
    key = _sp_key(cfg, sp_name, schema)
    iso = state.get(key)
    if not iso:
        return True, None
    try:
        last = datetime.fromisoformat(iso)
    except Exception:
        return True, None
    due = last + policy.ttl()
    return _now_utc() >= due, last


def mark_ran(cfg: Config, sp_name: str, schema: str) -> None:
    state = _load_state()
    key = _sp_key(cfg, sp_name, schema)
    state[key] = _now_utc().isoformat()
    _save_state(state)


def maybe_run_sp(cfg: Config, sp_name: str, schema: str = 'dbo', force: bool = False, policy: Optional[SPRunPolicy] = None) -> dict:
    policy = policy or SPRunPolicy()
    if force:
        execute_stored_procedure(cfg, sp_name, schema)
        mark_ran(cfg, sp_name, schema)
        return {"ran": True, "reason": "forced"}
    doit, last = should_run(cfg, sp_name, schema, policy)
    if not doit:
        return {"ran": False, "reason": f"recent (last run {last.isoformat()})"}
    execute_stored_procedure(cfg, sp_name, schema)
    mark_ran(cfg, sp_name, schema)
    return {"ran": True, "reason": "ttl_expired"}

