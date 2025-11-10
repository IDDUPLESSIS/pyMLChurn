from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import sys


def _to_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


@dataclass
class Config:
    server: str
    database: str
    auth: str = "windows"  # 'windows' or 'sql'
    username: Optional[str] = None
    password: Optional[str] = None
    odbc_driver: Optional[str] = None
    encrypt: bool = True
    trust_server_certificate: bool = True
    timeout: int = 30

    @staticmethod
    def from_env() -> "Config":
        # Load .env if present, searching common locations so EXE double-click works
        # 1) current working dir
        load_dotenv(override=False)
        # 2) alongside the executable (frozen) or module, and one level up
        try:
            candidates = []
            if getattr(sys, "frozen", False):
                exe_dir = Path(sys.executable).resolve().parent
                candidates = [exe_dir / ".env", exe_dir.parent / ".env"]
            else:
                here = Path(__file__).resolve().parent
                candidates = [here / ".env", here.parent / ".env"]
            for p in candidates:
                if p.exists():
                    load_dotenv(dotenv_path=p, override=False)
        except Exception:
            pass
        # 3) fallback: search upward from CWD
        try:
            fn = find_dotenv(usecwd=True)
            if fn:
                load_dotenv(dotenv_path=fn, override=False)
        except Exception:
            pass

        server = os.getenv("MSSQL_SERVER")
        database = os.getenv("MSSQL_DATABASE")
        if not server or not database:
            raise ValueError(
                "MSSQL_SERVER and MSSQL_DATABASE must be set (via .env or environment)."
            )

        auth = (os.getenv("MSSQL_AUTH") or "windows").strip().lower()
        if auth not in {"windows", "sql"}:
            raise ValueError("MSSQL_AUTH must be 'windows' or 'sql'")

        username = os.getenv("MSSQL_USERNAME") if auth == "sql" else None
        password = os.getenv("MSSQL_PASSWORD") if auth == "sql" else None

        odbc_driver = os.getenv("MSSQL_ODBC_DRIVER") or None
        encrypt = _to_bool(os.getenv("MSSQL_ENCRYPT"), True)
        trust = _to_bool(os.getenv("MSSQL_TRUST_CERT"), True)
        try:
            timeout = int(os.getenv("MSSQL_TIMEOUT") or 30)
        except ValueError:
            timeout = 30

        return Config(
            server=server,
            database=database,
            auth=auth,
            username=username,
            password=password,
            odbc_driver=odbc_driver,
            encrypt=encrypt,
            trust_server_certificate=trust,
            timeout=timeout,
        )
