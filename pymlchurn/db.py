from __future__ import annotations

from typing import Iterable, Optional, Dict, Any

import pandas as pd
import pyodbc
from urllib.parse import quote_plus
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc
import time

from .config import Config


def pick_driver(preferred: Optional[str] = None) -> str:
    installed = set(pyodbc.drivers())
    if preferred and preferred in installed:
        return preferred
    for d in (
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ):
        if d in installed:
            return d
    raise RuntimeError(
        f"No suitable SQL Server ODBC driver found. Installed: {sorted(installed)}"
    )


def build_connection_string(cfg: Config) -> str:
    driver = pick_driver(cfg.odbc_driver)
    base = [
        "DRIVER={" + driver + "}",
        f"SERVER={cfg.server}",
        f"DATABASE={cfg.database}",
        f"Encrypt={'yes' if cfg.encrypt else 'no'}",
        f"TrustServerCertificate={'yes' if cfg.trust_server_certificate else 'no'}",
        f"Connection Timeout={cfg.timeout}",
    ]

    if cfg.auth == "windows":
        base.append("Trusted_Connection=yes")
    else:
        if not cfg.username or not cfg.password:
            raise ValueError("Username and password required when MSSQL_AUTH=sql")
        base.append(f"UID={cfg.username}")
        base.append(f"PWD={cfg.password}")

    return ";".join(base)


def create_engine(cfg: Config) -> sa.Engine:
    conn_str = build_connection_string(cfg)
    url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn_str)
    engine = sa.create_engine(url, fast_executemany=True, pool_pre_ping=True)
    return engine


def query_dataframe(cfg: Config, sql: str, params: Optional[Iterable] = None, retries: int = 5, backoff: float = 2.0) -> pd.DataFrame:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(cfg)
            with engine.connect() as conn:
                return pd.read_sql_query(sql, conn, params=list(params or []))
        except (pyodbc.Error, sa_exc.SQLAlchemyError) as e:
            last_err = e
            if attempt >= retries:
                raise
            time.sleep(backoff * (2 ** (attempt - 1)))
    # Shouldn't reach here
    raise last_err  # type: ignore


def connectivity_info(cfg: Config, retries: int = 3, backoff: float = 1.5) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "server": cfg.server,
        "database": cfg.database,
        "driver": pick_driver(cfg.odbc_driver),
        "auth": cfg.auth,
    }
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(cfg)
            with engine.connect() as conn:
                version = conn.exec_driver_sql("SELECT @@VERSION").scalar()
                user = conn.exec_driver_sql("SELECT SUSER_SNAME()" ).scalar()
                info.update({
                    "sqlserver_version": version,
                    "user": user,
                })
                return info
        except (pyodbc.Error, sa_exc.SQLAlchemyError) as e:
            last_err = e
            if attempt >= retries:
                raise
            time.sleep(backoff * (2 ** (attempt - 1)))
    raise last_err  # type: ignore
    return info


def execute_stored_procedure(cfg: Config, sp_name: str, schema: str = "dbo", retries: int = 5, backoff: float = 2.0) -> None:
    call = f"SET NOCOUNT ON; EXEC [{schema}].[{sp_name}]"
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(cfg)
            with engine.begin() as conn:
                conn.exec_driver_sql(call)
                return
        except (pyodbc.Error, sa_exc.SQLAlchemyError) as e:
            last_err = e
            if attempt >= retries:
                raise
            time.sleep(backoff * (2 ** (attempt - 1)))
    raise last_err  # type: ignore
