from __future__ import annotations

from typing import Dict, Tuple
import re

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

from .db import create_engine
from .config import Config


def _sanitize(name: str) -> str:
    out = name.strip().lower()
    replacements = {
        " ": "_",
        "/": "_",
        "(": "_",
        ")": "",
        ":": "",
        ",": "",
        "%": "pct",
        "-": "_",
        "[": "",
        "]": "",
        "+": "plus",
    }
    for k, v in replacements.items():
        out = out.replace(k, v)
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_")


def _is_safe_name(name: str) -> bool:
    return re.match(r"^[A-Za-z][A-Za-z0-9]*$", name) is not None


def _map_dtypes(df: pd.DataFrame) -> Tuple[Dict[str, sa.types.TypeEngine], Dict[str, str]]:
    """Return SQLAlchemy dtype mapping and a rename map (sanitized names)."""
    # If all column names are already simple PascalCase (no spaces/underscores), keep them
    if all(_is_safe_name(c) for c in df.columns):
        rename: Dict[str, str] = {c: c for c in df.columns}
    else:
        rename = {c: _sanitize(c) for c in df.columns}
    dtypes: Dict[str, sa.types.TypeEngine] = {}

    for c in df.columns:
        sc = rename[c]
        key = re.sub(r"[^A-Za-z0-9]", "", c).lower()
        if key in {"customerid", "customer_id", "customer id"} or sc.lower() in {"customer_id", "customerid"}:
            dtypes[sc] = sa.Integer()
            continue
        if "snapshotdate" in key or sc.lower() == "snapshot_date":
            dtypes[sc] = sa.Date()
            continue
        if "dayssincelastpurchase" in key:
            dtypes[sc] = sa.Integer()
            continue
        if any(x in key for x in ["churnednow","predictedtochurn","actualchurned"]):
            dtypes[sc] = sa.Boolean()
            continue
        if "probabilitypct" in key:
            dtypes[sc] = sa.Numeric(5, 2)
            continue
        if "probability" in key:
            dtypes[sc] = sa.Numeric(9, 6)
            continue
        if any(x in key for x in ["why","reason","atrisk","status","outcome"]):
            dtypes[sc] = sa.types.NVARCHAR(length=None)
            continue
        if key == "createdon":
            dtypes[sc] = sa.DateTime()
            continue
        # fallback based on pandas dtype
        if pd.api.types.is_integer_dtype(df[c]):
            dtypes[sc] = sa.Integer()
        elif pd.api.types.is_bool_dtype(df[c]):
            dtypes[sc] = sa.Boolean()
        elif pd.api.types.is_float_dtype(df[c]):
            dtypes[sc] = sa.Float()
        else:
            dtypes[sc] = sa.types.NVARCHAR(length=None)
    return dtypes, rename


def create_table_if_missing(cfg: Config, df: pd.DataFrame, schema: str, table: str) -> Tuple[str, Dict[str, sa.types.TypeEngine]]:
    engine = create_engine(cfg)
    dtypes, rename = _map_dtypes(df)
    cols_sql = []
    for orig, sc in rename.items():
        typ = dtypes[sc]
        if isinstance(typ, sa.types.Boolean):
            decl = f"[{sc}] BIT NULL"
        elif isinstance(typ, sa.types.Integer):
            decl = f"[{sc}] INT NULL"
        elif isinstance(typ, sa.types.Date):
            decl = f"[{sc}] DATE NULL"
        elif isinstance(typ, sa.types.Numeric):
            # get precision/scale if provided
            precision = getattr(typ, 'precision', 18) or 18
            scale = getattr(typ, 'scale', 6) or 6
            decl = f"[{sc}] DECIMAL({precision},{scale}) NULL"
        elif isinstance(typ, sa.types.Float):
            decl = f"[{sc}] FLOAT NULL"
        elif isinstance(typ, sa.types.NVARCHAR):
            decl = f"[{sc}] NVARCHAR(MAX) NULL"
        else:
            decl = f"[{sc}] NVARCHAR(MAX) NULL"
        cols_sql.append(decl)
    cols_clause = ",\n    ".join(cols_sql)
    fq = f"[{schema}].[{table}]"
    ddl = f"""
IF OBJECT_ID(N'{fq}', N'U') IS NULL
BEGIN
    CREATE TABLE {fq} (
        {cols_clause}
    );
END
"""
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
    return fq, dtypes


def load_dataframe(cfg: Config, df: pd.DataFrame, schema: str, table: str, if_exists: str = "append") -> None:
    engine = create_engine(cfg)
    dtypes, rename = _map_dtypes(df)
    df2 = df.rename(columns=rename)
    df2.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        dtype=dtypes,
        method=None,
    )
