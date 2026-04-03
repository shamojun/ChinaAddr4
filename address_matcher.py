import re
import sqlite3
from dataclasses import dataclass
from typing import Dict, Optional

from opencc import OpenCC
from pypinyin import Style, lazy_pinyin

LEVELS = {
    "province": {
        "suffixes": ["特别行政区", "自治区", "省", "市"],
    },
    "city": {
        "suffixes": ["自治州", "地区", "盟", "市"],
    },
    "area": {
        "suffixes": ["自治县", "县", "区", "旗", "市", "林区", "特区"],
    },
    "street": {
        "suffixes": ["街道办事处", "街道", "镇", "乡", "苏木", "民族乡"],
    },
    "village": {
        "suffixes": ["居民委员会", "村民委员会", "居委会", "村委会", "社区", "小区", "村", "嘎查"],
    },
}

STREET_HINTS = list(LEVELS["street"]["suffixes"])
VILLAGE_HINTS = list(LEVELS["village"]["suffixes"])


def normalize(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s,，。\.、;；:：\-_()/（）【】\[\]<>《》“”\"']", "", str(text)).strip()


# --- override mojibake suffixes/normalize ---
LEVELS = {
    "province": {"suffixes": ["特别行政区", "自治区", "省", "市"]},
    "city": {"suffixes": ["自治区", "地区", "盟", "市", "州"]},
    "area": {"suffixes": ["自治区", "县", "区", "旗", "市", "林区", "特区"]},
    "street": {"suffixes": ["街道办事处", "街道", "镇", "乡", "苏木", "民族乡"]},
    "village": {"suffixes": ["居民委员会", "村民委员会", "居委会", "村委会", "社区", "小区", "村", "嘎查"]},
}
STREET_HINTS = list(LEVELS["street"]["suffixes"])
VILLAGE_HINTS = list(LEVELS["village"]["suffixes"])


def normalize(text: str) -> str:
    if not text:
        return ""
    return re.sub(
        r"[\s,，。．·、;；:：'\"“”‘’()（）\[\]{}<>《》【】—–\-_/\\]+",
        "",
        str(text),
    ).strip()


def ngram_tokens(text: str, sizes=(2, 3, 4), max_tokens: int = 50, include_full: bool = True):
    tokens = []
    if include_full and text:
        tokens.append(text)
    length = len(text)
    for size in sizes:
        if length < size:
            continue
        for idx in range(length - size + 1):
            tokens.append(text[idx : idx + size])
    seen = set()
    out = []
    for token in tokens:
        if token and token not in seen:
            out.append(token)
            seen.add(token)
            if len(out) >= max_tokens:
                break
    return out


def strip_suffix(name: str, suffixes) -> str:
    if not name:
        return ""
    for suffix in sorted(suffixes, key=len, reverse=True):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def extract_level_text(text: str, suffixes) -> str:
    if not text:
        return ""
    earliest = None
    end_at = None
    for suffix in suffixes:
        idx = text.find(suffix)
        if idx >= 0:
            if earliest is None or idx < earliest:
                earliest = idx
                end_at = idx + len(suffix)
    if end_at:
        return text[:end_at]
    return text


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    rows = len(a) + 1
    cols = len(b) + 1
    matrix = [[0] * cols for _ in range(rows)]
    for i in range(rows):
        matrix[i][0] = i
    for j in range(cols):
        matrix[0][j] = j
    for i in range(1, rows):
        for j in range(1, cols):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            matrix[i][j] = min(
                matrix[i - 1][j] + 1,
                matrix[i][j - 1] + 1,
                matrix[i - 1][j - 1] + cost,
            )
    return matrix[-1][-1]


def similarity(a: str, b: str) -> float:
    max_len = max(len(a), len(b))
    if not max_len:
        return 0.0
    return 1 - levenshtein(a, b) / max_len


def score_name(input_text: str, name: str, level: str) -> float:
    input_norm = normalize(input_text)
    name_norm = normalize(name)
    if not input_norm or not name_norm:
        return 0.0
    suffixes = LEVELS[level]["suffixes"]
    base_input = strip_suffix(input_norm, suffixes)
    base_name = strip_suffix(name_norm, suffixes)
    score = similarity(base_input, base_name)
    if input_norm.find(name_norm) >= 0 or input_norm.find(base_name) >= 0:
        score += 0.2
    if name_norm.find(input_norm) >= 0 or base_name.find(base_input) >= 0:
        score += 0.1
    return min(1.0, score)


def has_any_suffix(text: str, suffixes) -> bool:
    return any(suffix in text for suffix in suffixes)


def should_match_street(text: str, deep: bool) -> bool:
    norm = normalize(text)
    return deep or has_any_suffix(norm, STREET_HINTS) or has_any_suffix(norm, VILLAGE_HINTS)


def should_match_village(text: str, deep: bool) -> bool:
    norm = normalize(text)
    return deep or has_any_suffix(norm, VILLAGE_HINTS)


@dataclass
class MatchResult:
    code: str
    name: str
    score: float
    alias_name: Optional[str] = None


def pinyin_full(text: str) -> str:
    return "".join(lazy_pinyin(text, style=Style.NORMAL))


def pinyin_abbr(text: str) -> str:
    return "".join(lazy_pinyin(text, style=Style.FIRST_LETTER))


def fts5_enabled(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT sqlite_compileoption_used('SQLITE_ENABLE_FTS5')").fetchone()
    return bool(row and row[0] == 1)


class AddressMatcher:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.fts_enabled = fts5_enabled(self.conn) and self._fts_tables_ready()
        self.t2s = OpenCC("t2s")
        self.s2t = OpenCC("s2t")

    def _fts_tables_ready(self) -> bool:
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='address_fts'"
        ).fetchone()
        return bool(row)

    def _row_value(self, row: sqlite3.Row, key: str):
        return row[key] if key in row.keys() else None

    def _score_row(self, row: sqlite3.Row, input_text: str, level: str) -> float:
        input_norm = normalize(input_text)
        input_pinyin = pinyin_full(input_norm)
        input_abbr = pinyin_abbr(input_norm)
        input_jian = normalize(self.t2s.convert(input_norm))
        input_fan = normalize(self.s2t.convert(input_norm))

        row_name = self._row_value(row, "name") or ""
        base_score = score_name(input_norm, row_name, level)

        scores = [base_score]
        row_pinyin = self._row_value(row, "pinyin") or pinyin_full(row_name)
        row_abbr = self._row_value(row, "pinyin_abbr") or pinyin_abbr(row_name)
        row_jian = self._row_value(row, "name_jian") or normalize(self.t2s.convert(row_name))
        row_fan = self._row_value(row, "name_fan") or normalize(self.s2t.convert(row_name))

        if row_pinyin:
            scores.append(similarity(input_pinyin, row_pinyin))
        if row_abbr:
            scores.append(similarity(input_abbr, row_abbr))
        if row_jian:
            scores.append(score_name(input_jian, row_jian, level))
        if row_fan:
            scores.append(score_name(input_fan, row_fan, level))

        score = max(scores)
        if self._row_value(row, "alias_of"):
            score += 0.08
        return min(1.0, score)

    def _residual_text(self, input_text: str, parts: Dict[str, Optional[MatchResult]]) -> str:
        residual = normalize(input_text)
        for level, part in parts.items():
            if not part:
                continue
            name_norm = normalize(part.name)
            if name_norm:
                residual = residual.replace(name_norm, "")
            suffixes = LEVELS.get(level, {}).get("suffixes", [])
            base = strip_suffix(name_norm, suffixes)
            if base and base != name_norm:
                residual = residual.replace(base, "")
            if level in ("street", "village") and suffixes:
                extracted = extract_level_text(residual, suffixes)
                if extracted and extracted != residual:
                    residual = residual.replace(extracted, "")
        return residual

    def _official_name(self, level: str, code: str) -> Optional[str]:
        row = self.conn.execute(f"SELECT name FROM {level} WHERE code = ?", (code,)).fetchone()
        if row:
            return row["name"]
        return None

    def _get_row_by_code(self, table: str, code: str):
        return self.conn.execute(f"SELECT * FROM {table} WHERE code = ?", (code,)).fetchone()

    def _match_from_row(self, row: sqlite3.Row, score: float = 1.0) -> MatchResult:
        return MatchResult(code=row["code"], name=row["name"], score=score)

    def _finalize_row(self, row: sqlite3.Row, score: float, level: str) -> MatchResult:
        alias_of = self._row_value(row, "alias_of")
        name = self._row_value(row, "name")
        alias_name = None
        if alias_of:
            alias_name = name
            official = self._official_name(level, row["code"])
            if official:
                name = official
        return MatchResult(code=row["code"], name=name, score=score, alias_name=alias_name)

    def _rank_rows(self, rows, input_text: str, level: str):
        scored = []
        for row in rows:
            score = self._score_row(row, input_text, level)
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored

    def _pick_best(self, rows, input_text: str, level: str) -> Optional[MatchResult]:
        scored = self._rank_rows(rows, input_text, level)
        if not scored:
            return None
        score, row = scored[0]
        return self._finalize_row(row, score, level)

    def _pick_topn(self, rows, input_text: str, level: str, topn: int):
        scored = self._rank_rows(rows, input_text, level)
        results = []
        for score, row in scored[:topn]:
            results.append(self._finalize_row(row, score, level))
        return results

    def _fetch_all(self, table: str, where: Optional[Dict[str, str]] = None):
        if not where:
            sql = f"SELECT * FROM {table}"
            return self.conn.execute(sql).fetchall()
        clauses = []
        values = []
        for key, value in where.items():
            clauses.append(f"{key} = ?")
            values.append(value)
        sql = f"SELECT * FROM {table} WHERE " + " AND ".join(clauses)
        return self.conn.execute(sql, values).fetchall()

    def _fetch_all_limited(self, table: str, where: Optional[Dict[str, str]] = None, limit: int = 2000):
        if not where:
            sql = f"SELECT * FROM {table} LIMIT ?"
            return self.conn.execute(sql, (limit,)).fetchall()
        clauses = []
        values = []
        for key, value in where.items():
            clauses.append(f"{key} = ?")
            values.append(value)
        sql = f"SELECT * FROM {table} WHERE " + " AND ".join(clauses) + " LIMIT ?"
        return self.conn.execute(sql, values + [limit]).fetchall()

    def _fetch_index_limited(self, level: str, where: Optional[Dict[str, str]] = None, limit: int = 2000):
        clauses = ["level = ?"]
        values = [level]
        if where:
            for key, value in where.items():
                clauses.append(f"{key} = ?")
                values.append(value)
        sql = "SELECT * FROM address_index WHERE " + " AND ".join(clauses) + " LIMIT ?"
        return self.conn.execute(sql, values + [limit]).fetchall()

    def _fts_query(self, input_text: str) -> str:
        input_norm = normalize(input_text)
        input_pinyin = pinyin_full(input_norm)
        input_abbr = pinyin_abbr(input_norm)
        input_jian = normalize(self.t2s.convert(input_norm))
        input_fan = normalize(self.s2t.convert(input_norm))
        def sanitize(value: str) -> str:
            return value.replace('"', "")

        tokens = ngram_tokens(input_norm, sizes=(3,), max_tokens=40, include_full=True)

        parts = []
        for token in dict.fromkeys(tokens):
            parts.append(f"name_norm:{sanitize(token)}")
            parts.append(f"name:{sanitize(token)}")
            if input_jian:
                parts.append(f"name_jian:{sanitize(input_jian)}")
            if input_fan:
                parts.append(f"name_fan:{sanitize(input_fan)}")

        if input_pinyin:
            parts.append(f"pinyin:{sanitize(input_pinyin)}")
        if input_abbr:
            parts.append(f"pinyin_abbr:{sanitize(input_abbr)}")

        return " OR ".join(p for p in parts if p)

    def _fetch_candidates(self, level: str, input_text: str, where: Optional[Dict[str, str]] = None, debug: bool = False):
        if not self.fts_enabled:
            if debug:
                print(f"[debug] FTS disabled, fallback to table {level}")
            return self._fetch_all(level, where)

        query = self._fts_query(input_text)
        if not query:
            return []

        clauses = ["ai.level = ?"]
        values = [level]
        if where:
            for key, value in where.items():
                clauses.append(f"ai.{key} = ?")
                values.append(value)

        sql = (
            "SELECT ai.* FROM address_fts f "
            "JOIN address_index ai ON ai.id = f.rowid "
            f"WHERE address_fts MATCH ? AND {' AND '.join(clauses)} "
            "LIMIT 200"
        )
        if debug:
            print(f"[debug] FTS query level={level} where={where} query={query}")
        rows = self.conn.execute(sql, [query] + values).fetchall()
        if rows:
            if debug:
                print(f"[debug] FTS hit {len(rows)} rows for level={level}")
            return rows
        if debug:
            print(f"[debug] FTS empty, fallback LIKE for level={level}")
        rows = self._fetch_like_candidates(level, input_text, where, debug=debug)
        if rows:
            return rows
        if where:
            if debug:
                print(f"[debug] LIKE empty, fallback ALL for level={level} where={where}")
            return self._fetch_index_limited(level, where, limit=2000)
        return rows

    def _fetch_like_candidates(self, level: str, input_text: str, where: Optional[Dict[str, str]] = None, debug: bool = False):
        input_norm = normalize(input_text)
        input_jian = normalize(self.t2s.convert(input_norm))
        input_fan = normalize(self.s2t.convert(input_norm))
        tokens = ngram_tokens(input_norm, sizes=(2, 3, 4), max_tokens=60, include_full=True)

        clauses = ["level = ?"]
        values = [level]
        if where:
            for key, value in where.items():
                clauses.append(f"{key} = ?")
                values.append(value)

        like_clauses = []
        like_values = []
        for token in dict.fromkeys(tokens):
            for key in ["name", "name_norm"]:
                like_clauses.append(f"{key} LIKE ?")
                like_values.append(f"%{token}%")
        if input_jian and input_jian != input_norm:
            like_clauses.append("name_jian LIKE ?")
            like_values.append(f"%{input_jian}%")
        if input_fan and input_fan != input_norm:
            like_clauses.append("name_fan LIKE ?")
            like_values.append(f"%{input_fan}%")

        sql = (
            "SELECT * FROM address_index "
            f"WHERE {' AND '.join(clauses)} "
            f"AND ({' OR '.join(like_clauses)}) "
            "LIMIT 500"
        )
        if debug:
            print(f"[debug] LIKE level={level} where={where} norm={input_norm} jian={input_jian} fan={input_fan}")
        return self.conn.execute(sql, values + like_values).fetchall()

    def match_address(self, input_text: str, deep: bool = False, thresholds: Optional[Dict] = None, debug: bool = False):
        text = normalize(input_text)
        if not text:
            return {
                "input": input_text,
                "matchedLevel": "none",
                "score": 0,
                "fullAddress": "",
            }

        limits = {
            "province": 0.2,
            "city": 0.25,
            "area": 0.28,
            "street": 0.33,
            "village": 0.35,
        }
        if thresholds:
            limits.update(thresholds)

        if debug:
            print(f"[debug] match_address input={input_text} deep={deep}")
        province = self._pick_best(self._fetch_candidates("province", text, debug=debug), text, "province")
        if not province or province.score < limits["province"]:
            city_fallback = self._pick_best(self._fetch_candidates("city", text, debug=debug), text, "city")
            if city_fallback and city_fallback.score >= limits["city"]:
                city = city_fallback
                city_row = self._get_row_by_code("city", city.code)
                if city_row:
                    province_row = self._get_row_by_code("province", city_row["provinceCode"])
                    province = self._match_from_row(province_row, 1.0) if province_row else None
                else:
                    province = None
                area = self._pick_best(
                    self._fetch_candidates("area", text, {"cityCode": city.code}, debug=debug), text, "area"
                )
                if not area or area.score < limits["area"]:
                    return self._build_result(input_text, province, city, None, None, None)
                street = None
                if should_match_street(text, deep):
                    street_input = self._residual_text(
                        input_text, {"province": province, "city": city, "area": area}
                    )
                    street_input = extract_level_text(street_input, STREET_HINTS)
                    if street_input:
                        street = self._pick_best(
                            self._fetch_candidates("street", street_input, {"areaCode": area.code}, debug=debug),
                            street_input,
                            "street",
                        )
                    if not street or street.score < limits["street"]:
                        street = None
                village = None
                if street and should_match_village(text, deep):
                    village_input = self._residual_text(
                        input_text,
                        {"province": province, "city": city, "area": area, "street": street},
                    )
                    village_input = extract_level_text(village_input, VILLAGE_HINTS)
                    if village_input:
                        village = self._pick_best(
                            self._fetch_candidates(
                                "village", village_input, {"streetCode": street.code}, debug=debug
                            ),
                            village_input,
                            "village",
                        )
                    if not village or village.score < limits["village"]:
                        village = None
                return self._build_result(input_text, province, city, area, street, village)

            return {
                "input": input_text,
                "matchedLevel": "none",
                "score": province.score if province else 0,
                "fullAddress": "",
            }

        city = self._pick_best(
            self._fetch_candidates("city", text, {"provinceCode": province.code}, debug=debug), text, "city"
        )
        if not city or city.score < limits["city"]:
            return self._build_result(input_text, province, None, None, None, None)

        area = self._pick_best(
            self._fetch_candidates("area", text, {"cityCode": city.code}, debug=debug), text, "area"
        )
        if not area or area.score < limits["area"]:
            return self._build_result(input_text, province, city, None, None, None)

        street = None
        if should_match_street(text, deep):
            street_input = self._residual_text(input_text, {"province": province, "city": city, "area": area})
            street_input = extract_level_text(street_input, STREET_HINTS)
            if street_input:
                street = self._pick_best(
                    self._fetch_candidates("street", street_input, {"areaCode": area.code}, debug=debug),
                    street_input,
                    "street",
                )
            if not street or street.score < limits["street"]:
                street = None

        village = None
        if street and should_match_village(text, deep):
            village_input = self._residual_text(
                input_text, {"province": province, "city": city, "area": area, "street": street}
            )
            village_input = extract_level_text(village_input, VILLAGE_HINTS)
            if village_input:
                village = self._pick_best(
                    self._fetch_candidates("village", village_input, {"streetCode": street.code}, debug=debug),
                    village_input,
                    "village",
                )
            if not village or village.score < limits["village"]:
                village = None

        return self._build_result(input_text, province, city, area, street, village)

    def match_topn(self, input_text: str, topn: int = 5, deep: bool = False, thresholds: Optional[Dict] = None, debug: bool = False):
        text = normalize(input_text)
        if not text:
            return {
                "input": input_text,
                "matchedLevel": "none",
                "score": 0,
                "fullAddress": "",
                "candidates": [],
            }

        limits = {
            "province": 0.2,
            "city": 0.25,
            "area": 0.28,
            "street": 0.33,
            "village": 0.35,
        }
        if thresholds:
            limits.update(thresholds)

        if debug:
            print(f"[debug] match_topn input={input_text} deep={deep} topn={topn}")
        province = self._pick_best(self._fetch_candidates("province", text, debug=debug), text, "province")
        if not province or province.score < limits["province"]:
            city_fallback = self._pick_best(self._fetch_candidates("city", text, debug=debug), text, "city")
            if city_fallback and city_fallback.score >= limits["city"]:
                city = city_fallback
                city_row = self._get_row_by_code("city", city.code)
                province_row = self._get_row_by_code("province", city_row["provinceCode"]) if city_row else None
                province = self._match_from_row(province_row, 1.0) if province_row else None
                area = self._pick_best(
                    self._fetch_candidates("area", text, {"cityCode": city.code}, debug=debug), text, "area"
                )
                if not area or area.score < limits["area"]:
                    return self._build_topn_result(input_text, province, city, None, None, None, topn, debug=debug)
                street = None
                if should_match_street(text, deep):
                    street_input = self._residual_text(
                        input_text, {"province": province, "city": city, "area": area}
                    )
                    street_input = extract_level_text(street_input, STREET_HINTS)
                    if street_input:
                        street = self._pick_best(
                            self._fetch_candidates("street", street_input, {"areaCode": area.code}, debug=debug),
                            street_input,
                            "street",
                        )
                    if not street or street.score < limits["street"]:
                        street = None
                village = None
                if street and should_match_village(text, deep):
                    village_input = self._residual_text(
                        input_text, {"province": province, "city": city, "area": area, "street": street}
                    )
                    village_input = extract_level_text(village_input, VILLAGE_HINTS)
                    if village_input:
                        village = self._pick_best(
                            self._fetch_candidates(
                                "village", village_input, {"streetCode": street.code}, debug=debug
                            ),
                            village_input,
                            "village",
                        )
                    if not village or village.score < limits["village"]:
                        village = None
                return self._build_topn_result(input_text, province, city, area, street, village, topn, debug=debug)

            return {
                "input": input_text,
                "matchedLevel": "none",
                "score": province.score if province else 0,
                "fullAddress": "",
                "candidates": [],
            }

        city = self._pick_best(
            self._fetch_candidates("city", text, {"provinceCode": province.code}, debug=debug), text, "city"
        )
        if not city or city.score < limits["city"]:
            return self._build_topn_result(input_text, province, None, None, None, None, topn)

        area = self._pick_best(
            self._fetch_candidates("area", text, {"cityCode": city.code}, debug=debug), text, "area"
        )
        if not area or area.score < limits["area"]:
            return self._build_topn_result(input_text, province, city, None, None, None, topn)

        street = None
        if should_match_street(text, deep):
            street_input = self._residual_text(input_text, {"province": province, "city": city, "area": area})
            street_input = extract_level_text(street_input, STREET_HINTS)
            if street_input:
                street = self._pick_best(
                    self._fetch_candidates("street", street_input, {"areaCode": area.code}, debug=debug),
                    street_input,
                    "street",
                )
            if not street or street.score < limits["street"]:
                street = None

        village = None
        if street and should_match_village(text, deep):
            village_input = self._residual_text(
                input_text, {"province": province, "city": city, "area": area, "street": street}
            )
            village_input = extract_level_text(village_input, VILLAGE_HINTS)
            if village_input:
                village = self._pick_best(
                    self._fetch_candidates("village", village_input, {"streetCode": street.code}, debug=debug),
                    village_input,
                    "village",
                )
            if not village or village.score < limits["village"]:
                village = None

        return self._build_topn_result(input_text, province, city, area, street, village, topn, debug=debug)

    def _build_topn_result(self, input_text, province, city, area, street, village, topn, debug: bool = False):
        level = (
            "village"
            if village
            else "street"
            if street
            else "area"
            if area
            else "city"
            if city
            else "province"
        )

        if level == "village":
            rows = self._fetch_candidates("village", input_text, {"streetCode": street.code}, debug=debug)
        elif level == "street":
            rows = self._fetch_candidates("street", input_text, {"areaCode": area.code}, debug=debug)
        elif level == "area":
            rows = self._fetch_candidates("area", input_text, {"cityCode": city.code}, debug=debug)
        elif level == "city":
            rows = self._fetch_candidates("city", input_text, {"provinceCode": province.code}, debug=debug)
        else:
            rows = self._fetch_candidates("province", input_text, debug=debug)

        topn_rows = self._pick_topn(rows, input_text, level, topn)

        def _to_dict(obj):
            if not obj:
                return None
            data = {"code": obj.code, "name": obj.name, "score": obj.score}
            if obj.alias_name:
                data["aliasMatched"] = obj.alias_name
            return data

        def _candidate_full_address(candidate_name: str) -> str:
            if level == "province":
                parts = [candidate_name]
            elif level == "city":
                parts = [province.name if province else None, candidate_name]
            elif level == "area":
                parts = [province.name if province else None, city.name if city else None, candidate_name]
            elif level == "street":
                parts = [
                    province.name if province else None,
                    city.name if city else None,
                    area.name if area else None,
                    candidate_name,
                ]
            else:
                parts = [
                    province.name if province else None,
                    city.name if city else None,
                    area.name if area else None,
                    street.name if street else None,
                    candidate_name,
                ]
            return "".join(part for part in parts if part)

        candidates = []
        for item in topn_rows:
            candidates.append(
                {
                    "level": level,
                    "code": item.code,
                    "name": item.name,
                    "score": item.score,
                    "aliasMatched": item.alias_name,
                    "fullAddress": _candidate_full_address(item.name),
                }
            )

        best = topn_rows[0] if topn_rows else None
        full_address = "".join(part.name for part in [province, city, area, street, village] if part)
        residual = self._residual_text(
            input_text,
            {"province": province, "city": city, "area": area, "street": street, "village": village},
        )
        if residual:
            full_address = f"{full_address}{residual}"

        return {
            "input": input_text,
            "matchedLevel": level,
            "score": best.score if best else 0,
            "fullAddress": full_address,
            "province": _to_dict(province),
            "city": _to_dict(city),
            "area": _to_dict(area),
            "street": _to_dict(street),
            "village": _to_dict(village),
            "candidates": candidates,
        }

    def _build_result(self, input_text, province, city, area, street, village):
        full_address = "".join(part.name for part in [province, city, area, street, village] if part)
        if village:
            matched_level = "village"
            score = village.score
        elif street:
            matched_level = "street"
            score = street.score
        elif area:
            matched_level = "area"
            score = area.score
        elif city:
            matched_level = "city"
            score = city.score
        else:
            matched_level = "province"
            score = province.score

        def _to_dict(obj):
            if not obj:
                return None
            data = {"code": obj.code, "name": obj.name, "score": obj.score}
            if obj.alias_name:
                data["aliasMatched"] = obj.alias_name
            return data

        residual = self._residual_text(
            input_text,
            {"province": province, "city": city, "area": area, "street": street, "village": village},
        )
        if residual:
            full_address = f"{full_address}{residual}"

        return {
            "input": input_text,
            "matchedLevel": matched_level,
            "score": score,
            "fullAddress": full_address,
            "province": _to_dict(province),
            "city": _to_dict(city),
            "area": _to_dict(area),
            "street": _to_dict(street),
            "village": _to_dict(village),
        }


def create_matcher(db_path: str) -> AddressMatcher:
    return AddressMatcher(db_path)
