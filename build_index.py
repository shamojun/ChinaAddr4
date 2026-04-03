import json
import os
import sqlite3
from typing import Dict, Iterable, List, Optional

from opencc import OpenCC
from pypinyin import Style, lazy_pinyin

from address_matcher import LEVELS, normalize


DB_PATH = os.path.join(os.path.dirname(__file__), "dist", "data.sqlite")
ALIAS_PATH = os.path.join(os.path.dirname(__file__), "dist", "aliases.json")


def fts5_enabled(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT sqlite_compileoption_used('SQLITE_ENABLE_FTS5')").fetchone()
    return bool(row and row[0] == 1)


def pinyin_full(text: str) -> str:
    return "".join(lazy_pinyin(text, style=Style.NORMAL))


def pinyin_abbr(text: str) -> str:
    return "".join(lazy_pinyin(text, style=Style.FIRST_LETTER))


def load_aliases(path: str) -> List[Dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError("aliases.json must be a list")
    return data


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS address_index (
            id INTEGER PRIMARY KEY,
            level TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            name_norm TEXT NOT NULL,
            pinyin TEXT NOT NULL,
            pinyin_abbr TEXT NOT NULL,
            name_jian TEXT NOT NULL,
            name_fan TEXT NOT NULL,
            provinceCode TEXT,
            cityCode TEXT,
            areaCode TEXT,
            streetCode TEXT,
            alias_of TEXT
        )
        """
    )
    conn.execute("DELETE FROM address_index")

    conn.execute("DROP TABLE IF EXISTS address_fts")
    conn.execute(
        """
        CREATE VIRTUAL TABLE address_fts USING fts5(
            name,
            name_norm,
            pinyin,
            pinyin_abbr,
            name_jian,
            name_fan,
            content='address_index',
            content_rowid='id',
            tokenize='trigram'
        )
        """
    )


def iter_rows(conn: sqlite3.Connection, table: str, fields: Iterable[str]):
    columns = ", ".join(fields)
    return conn.execute(f"SELECT {columns} FROM {table}").fetchall()


def index_rows(conn: sqlite3.Connection, rows: List[Dict]) -> None:
    conn.executemany(
        """
        INSERT INTO address_index (
            level, code, name, name_norm, pinyin, pinyin_abbr,
            name_jian, name_fan, provinceCode, cityCode, areaCode, streetCode, alias_of
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["level"],
                row["code"],
                row["name"],
                row["name_norm"],
                row["pinyin"],
                row["pinyin_abbr"],
                row["name_jian"],
                row["name_fan"],
                row.get("provinceCode"),
                row.get("cityCode"),
                row.get("areaCode"),
                row.get("streetCode"),
                row.get("alias_of"),
            )
            for row in rows
        ],
    )
    conn.execute("INSERT INTO address_fts(address_fts) VALUES('rebuild')")


def row_value(row: sqlite3.Row, key: str):
    return row[key] if key in row.keys() else None


def build_index(db_path: str, alias_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    if not fts5_enabled(conn):
        raise RuntimeError("SQLite was compiled without FTS5 support.")

    ensure_schema(conn)

    t2s = OpenCC("t2s")
    s2t = OpenCC("s2t")

    rows: List[Dict] = []
    code_maps: Dict[str, Dict[str, sqlite3.Row]] = {
        "province": {},
        "city": {},
        "area": {},
        "street": {},
        "village": {},
    }

    table_fields = {
        "province": ["code", "name"],
        "city": ["code", "name", "provinceCode"],
        "area": ["code", "name", "cityCode", "provinceCode"],
        "street": ["code", "name", "areaCode", "provinceCode", "cityCode"],
        "village": ["code", "name", "streetCode", "provinceCode", "cityCode", "areaCode"],
    }

    for level, fields in table_fields.items():
        for row in iter_rows(conn, level, fields):
            code_maps[level][row["code"]] = row
            name = row["name"]
            row_data = {
                "level": level,
                "code": row["code"],
                "name": name,
                "name_norm": normalize(name),
                "pinyin": pinyin_full(name),
                "pinyin_abbr": pinyin_abbr(name),
                "name_jian": normalize(t2s.convert(name)),
                "name_fan": normalize(s2t.convert(name)),
                "provinceCode": row_value(row, "provinceCode"),
                "cityCode": row_value(row, "cityCode"),
                "areaCode": row_value(row, "areaCode"),
                "streetCode": row_value(row, "streetCode"),
                "alias_of": None,
            }
            rows.append(row_data)

    aliases = load_aliases(alias_path)
    for alias in aliases:
        alias_name = alias.get("alias")
        alias_code = alias.get("code")
        alias_level = alias.get("level")
        if not alias_name or not alias_code or alias_level not in code_maps:
            continue
        target = code_maps[alias_level].get(alias_code)
        if not target:
            continue
        row_data = {
            "level": alias_level,
            "code": alias_code,
            "name": alias_name,
            "name_norm": normalize(alias_name),
            "pinyin": pinyin_full(alias_name),
            "pinyin_abbr": pinyin_abbr(alias_name),
            "name_jian": normalize(t2s.convert(alias_name)),
            "name_fan": normalize(s2t.convert(alias_name)),
            "provinceCode": row_value(target, "provinceCode"),
            "cityCode": row_value(target, "cityCode"),
            "areaCode": row_value(target, "areaCode"),
            "streetCode": row_value(target, "streetCode"),
            "alias_of": alias_code,
        }
        rows.append(row_data)

    index_rows(conn, rows)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    build_index(DB_PATH, ALIAS_PATH)
