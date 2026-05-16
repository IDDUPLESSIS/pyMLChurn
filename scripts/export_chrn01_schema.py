from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pyodbc

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pymlchurn.config import Config
from pymlchurn.db import build_connection_string


DEFAULT_OUT = ROOT / "sql" / "chrn01"


def quote_name(name: str) -> str:
    return "[" + str(name).replace("]", "]]") + "]"


def safe_file_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_")


def rows(cursor: pyodbc.Cursor, sql: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
    cursor.execute(sql, tuple(params))
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def scalar(cursor: pyodbc.Cursor, sql: str, params: Iterable[Any] = ()) -> Any:
    cursor.execute(sql, tuple(params))
    row = cursor.fetchone()
    return row[0] if row else None


def data_type(row: Dict[str, Any]) -> str:
    typ = str(row["type_name"])
    max_len = row["max_length"]
    precision = row["precision"]
    scale = row["scale"]

    if typ in {"varchar", "char", "varbinary", "binary"}:
        length = "MAX" if int(max_len) == -1 else str(int(max_len))
        return f"{typ}({length})"
    if typ in {"nvarchar", "nchar"}:
        length = "MAX" if int(max_len) == -1 else str(int(max_len) // 2)
        return f"{typ}({length})"
    if typ in {"decimal", "numeric"}:
        return f"{typ}({int(precision)},{int(scale)})"
    if typ in {"datetime2", "datetimeoffset", "time"}:
        return f"{typ}({int(scale)})"
    if typ in {"float"} and int(precision) != 53:
        return f"{typ}({int(precision)})"
    return typ


def column_definition(row: Dict[str, Any]) -> str:
    name = quote_name(row["column_name"])
    if row.get("computed_definition"):
        persisted = " PERSISTED" if row.get("is_persisted") else ""
        return f"    {name} AS {row['computed_definition']}{persisted}"

    parts = [f"    {name}", data_type(row)]
    if row.get("is_identity"):
        seed = row.get("seed_value")
        increment = row.get("increment_value")
        parts.append(f"IDENTITY({int(seed)},{int(increment)})")
    if row.get("collation_name"):
        parts.append(f"COLLATE {row['collation_name']}")
    parts.append("NULL" if row.get("is_nullable") else "NOT NULL")
    if row.get("default_definition"):
        parts.append(f"CONSTRAINT {quote_name(row['default_name'])} DEFAULT {row['default_definition']}")
    return " ".join(parts)


def comma_lines(items: List[str]) -> str:
    return ",\n".join(items)


def constraint_column_list(items: List[Dict[str, Any]]) -> str:
    return ", ".join(
        f"{quote_name(r['column_name'])} {'DESC' if r.get('is_descending_key') else 'ASC'}"
        for r in items
    )


def script_table(cursor: pyodbc.Cursor, schema: str, table: str) -> str:
    fq = f"{quote_name(schema)}.{quote_name(table)}"
    columns = rows(
        cursor,
        """
        SELECT
            c.column_id,
            c.name AS column_name,
            ty.name AS type_name,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.collation_name,
            dc.name AS default_name,
            dc.definition AS default_definition,
            cc.definition AS computed_definition,
            cc.is_persisted,
            ic.seed_value,
            ic.increment_value,
            CASE WHEN ic.column_id IS NULL THEN 0 ELSE 1 END AS is_identity
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.columns c ON c.object_id = t.object_id
        JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        LEFT JOIN sys.default_constraints dc
            ON dc.parent_object_id = c.object_id
           AND dc.parent_column_id = c.column_id
        LEFT JOIN sys.computed_columns cc
            ON cc.object_id = c.object_id
           AND cc.column_id = c.column_id
        LEFT JOIN sys.identity_columns ic
            ON ic.object_id = c.object_id
           AND ic.column_id = c.column_id
        WHERE s.name = ? AND t.name = ?
        ORDER BY c.column_id;
        """,
        (schema, table),
    )

    body = [column_definition(c) for c in columns]

    pk = rows(
        cursor,
        """
        SELECT
            kc.name AS constraint_name,
            i.type_desc,
            ic.key_ordinal,
            c.name AS column_name,
            ic.is_descending_key
        FROM sys.key_constraints kc
        JOIN sys.indexes i
            ON i.object_id = kc.parent_object_id
           AND i.index_id = kc.unique_index_id
        JOIN sys.index_columns ic
            ON ic.object_id = i.object_id
           AND ic.index_id = i.index_id
           AND ic.key_ordinal > 0
        JOIN sys.columns c
            ON c.object_id = ic.object_id
           AND c.column_id = ic.column_id
        JOIN sys.tables t ON t.object_id = kc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ? AND kc.type = 'PK'
        ORDER BY ic.key_ordinal;
        """,
        (schema, table),
    )
    if pk:
        body.append(
            "    CONSTRAINT "
            f"{quote_name(pk[0]['constraint_name'])} PRIMARY KEY {pk[0]['type_desc']} "
            f"({constraint_column_list(pk)})"
        )

    script = [
        f"IF OBJECT_ID(N'{schema}.{table}', N'U') IS NULL",
        "BEGIN",
        f"CREATE TABLE {fq}",
        "(",
        comma_lines(body),
        ");",
        "END",
        "GO",
        "",
    ]

    checks = rows(
        cursor,
        """
        SELECT cc.name, cc.definition
        FROM sys.check_constraints cc
        JOIN sys.tables t ON t.object_id = cc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ?
        ORDER BY cc.name;
        """,
        (schema, table),
    )
    for ck in checks:
        script.extend(
            [
                f"IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'{ck['name']}' AND parent_object_id = OBJECT_ID(N'{schema}.{table}'))",
                f"    ALTER TABLE {fq} ADD CONSTRAINT {quote_name(ck['name'])} CHECK {ck['definition']};",
                "GO",
                "",
            ]
        )

    unique_constraints = rows(
        cursor,
        """
        SELECT
            kc.name AS constraint_name,
            i.type_desc,
            ic.key_ordinal,
            c.name AS column_name,
            ic.is_descending_key
        FROM sys.key_constraints kc
        JOIN sys.indexes i
            ON i.object_id = kc.parent_object_id
           AND i.index_id = kc.unique_index_id
        JOIN sys.index_columns ic
            ON ic.object_id = i.object_id
           AND ic.index_id = i.index_id
           AND ic.key_ordinal > 0
        JOIN sys.columns c
            ON c.object_id = ic.object_id
           AND c.column_id = ic.column_id
        JOIN sys.tables t ON t.object_id = kc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ? AND kc.type = 'UQ'
        ORDER BY kc.name, ic.key_ordinal;
        """,
        (schema, table),
    )
    grouped_unique: Dict[str, List[Dict[str, Any]]] = {}
    for row in unique_constraints:
        grouped_unique.setdefault(row["constraint_name"], []).append(row)
    for name, items in grouped_unique.items():
        script.extend(
            [
                f"IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'{name}' AND parent_object_id = OBJECT_ID(N'{schema}.{table}'))",
                f"    ALTER TABLE {fq} ADD CONSTRAINT {quote_name(name)} UNIQUE {items[0]['type_desc']} ({constraint_column_list(items)});",
                "GO",
                "",
            ]
        )

    indexes = rows(
        cursor,
        """
        SELECT
            i.name AS index_name,
            i.type_desc,
            i.is_unique,
            i.has_filter,
            i.filter_definition,
            ic.key_ordinal,
            ic.index_column_id,
            ic.is_included_column,
            ic.is_descending_key,
            c.name AS column_name
        FROM sys.indexes i
        JOIN sys.tables t ON t.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.index_columns ic
            ON ic.object_id = i.object_id
           AND ic.index_id = i.index_id
        JOIN sys.columns c
            ON c.object_id = ic.object_id
           AND c.column_id = ic.column_id
        WHERE s.name = ?
          AND t.name = ?
          AND i.is_hypothetical = 0
          AND i.type > 0
          AND i.is_primary_key = 0
          AND i.is_unique_constraint = 0
        ORDER BY i.name, ic.key_ordinal, ic.index_column_id;
        """,
        (schema, table),
    )
    grouped_indexes: Dict[str, List[Dict[str, Any]]] = {}
    for row in indexes:
        grouped_indexes.setdefault(row["index_name"], []).append(row)
    for name, items in grouped_indexes.items():
        keys = [r for r in items if not r["is_included_column"]]
        includes = [r for r in items if r["is_included_column"]]
        unique = "UNIQUE " if items[0]["is_unique"] else ""
        key_sql = constraint_column_list(keys)
        include_sql = ""
        if includes:
            include_sql = " INCLUDE (" + ", ".join(quote_name(r["column_name"]) for r in includes) + ")"
        filter_sql = f" WHERE {items[0]['filter_definition']}" if items[0]["has_filter"] else ""
        script.extend(
            [
                f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{name}' AND object_id = OBJECT_ID(N'{schema}.{table}'))",
                f"    CREATE {unique}{items[0]['type_desc']} INDEX {quote_name(name)} ON {fq} ({key_sql}){include_sql}{filter_sql};",
                "GO",
                "",
            ]
        )

    return "\n".join(script).rstrip() + "\n"


def script_module(cursor: pyodbc.Cursor, schema: str, name: str, kind: str) -> str:
    row = rows(
        cursor,
        """
        SELECT
            o.name,
            o.type_desc,
            m.definition,
            sm.uses_ansi_nulls,
            sm.uses_quoted_identifier
        FROM sys.objects o
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        JOIN sys.sql_modules m ON m.object_id = o.object_id
        LEFT JOIN sys.sql_modules sm ON sm.object_id = o.object_id
        WHERE s.name = ? AND o.name = ?;
        """,
        (schema, name),
    )[0]
    definition = str(row["definition"]).replace("\r\n", "\n").rstrip()
    object_type = "V" if kind == "views" else "P"
    return "\n".join(
        [
            f"SET ANSI_NULLS {'ON' if row['uses_ansi_nulls'] else 'OFF'}",
            "GO",
            f"SET QUOTED_IDENTIFIER {'ON' if row['uses_quoted_identifier'] else 'OFF'}",
            "GO",
            f"IF OBJECT_ID(N'{schema}.{name}', N'{object_type}') IS NOT NULL",
            f"    DROP {'VIEW' if kind == 'views' else 'PROCEDURE'} {quote_name(schema)}.{quote_name(name)};",
            "GO",
            definition,
            "GO",
            "",
        ]
    )


def clear_sql_files(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for item in path.glob("*.sql"):
        item.unlink()


def write_text(path: Path, content: str) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(content)


def export_schema(schema: str, out_dir: Path) -> None:
    cfg = Config.from_env()
    conn = pyodbc.connect(build_connection_string(cfg))
    try:
        cursor = conn.cursor()
        for child in ("tables", "views", "procedures"):
            clear_sql_files(out_dir / child)

        object_rows = rows(
            cursor,
            """
            SELECT s.name AS schema_name, o.name, o.type, o.type_desc
            FROM sys.objects o
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ?
              AND o.is_ms_shipped = 0
              AND o.type IN ('U', 'V', 'P')
            ORDER BY o.type, o.name;
            """,
            (schema,),
        )

        manifest: List[str] = [
            f"# SQL export for [{schema}]",
            "",
            f"Source database: {cfg.database}",
            "",
            "| Type | Object | File |",
            "| --- | --- | --- |",
        ]

        for obj in object_rows:
            name = obj["name"]
            obj_type = str(obj["type"]).strip()
            if obj_type == "U":
                child = "tables"
                content = script_table(cursor, schema, name)
            elif obj_type == "V":
                child = "views"
                content = script_module(cursor, schema, name, "views")
            elif obj_type == "P":
                child = "procedures"
                content = script_module(cursor, schema, name, "procedures")
            else:
                continue

            rel = Path(child) / f"{safe_file_name(schema)}.{safe_file_name(name)}.sql"
            target = out_dir / rel
            write_text(target, content)
            manifest.append(f"| {obj['type_desc']} | [{schema}].[{name}] | `{rel.as_posix()}` |")

        write_text(out_dir / "README.md", "\n".join(manifest) + "\n")
        print(f"Exported {len(object_rows)} objects to {out_dir}")
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Export SQL Server schema objects to repo SQL files.")
    parser.add_argument("--schema", default="chrn01", help="SQL schema to export.")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT), help="Destination folder.")
    args = parser.parse_args()

    export_schema(args.schema, Path(args.out_dir).resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
