import calendar
import glob
import os
import re
import pyarrow as pa

_F1_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$", re.ASCII)
_F2_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})([+-])(\d{2}):(\d{2})$",
    re.ASCII,
)
_DIGITS_RE = re.compile(r"^\d+$", re.ASCII)


def parse_ts_ms(val: str | None) -> int | None:
    if not val:
        return None
    if val.isascii() and val.isdigit() and _DIGITS_RE.match(val):
        ms = int(val)
        return None if ms > 9_223_372_036_854_775_807 else ms
    m1 = _F1_RE.match(val)
    if m1:
        yr, mo, dy, hr, mi, sc = map(int, m1.groups())
        if not (1 <= mo <= 12 and 0 <= hr <= 23 and 0 <= mi <= 59 and 0 <= sc <= 59):
            return None
        _, max_day = calendar.monthrange(yr, mo)
        if not (1 <= dy <= max_day):
            return None
        return calendar.timegm((yr, mo, dy, hr, mi, sc)) * 1000
    m2 = _F2_RE.match(val)
    if m2:
        yr, mo, dy, hr, mi, sc, sign, off_h, off_m = m2.groups()
        yr, mo, dy, hr, mi, sc = int(yr), int(mo), int(dy), int(hr), int(mi), int(sc)
        off_h, off_m = int(off_h), int(off_m)
        if not (
            1 <= mo <= 12 and 0 <= hr <= 23 and 0 <= mi <= 59 and 0 <= sc <= 59
            and 0 <= off_h <= 23 and 0 <= off_m <= 59
        ):
            return None
        _, max_day = calendar.monthrange(yr, mo)
        if not (1 <= dy <= max_day):
            return None
        base_sec = calendar.timegm((yr, mo, dy, hr, mi, sc))
        offset_sec = off_h * 3600 + off_m * 60
        base_sec = base_sec - offset_sec if sign == "+" else base_sec + offset_sec
        return base_sec * 1000
    return None


def parse_strict_csv_line(line: str, expected_cols: int) -> list[str | None] | None:
    if not line:
        return None
    raw_fields = line.split(",")
    if len(raw_fields) != expected_cols:
        return None
    parsed: list[str | None] = []
    for f in raw_fields:
        if len(f) >= 2 and f.startswith('"') and f.endswith('"'):
            inner = f.removeprefix('"').removesuffix('"')
            if '"' in inner:
                return None
            parsed.append(None if inner == "" else inner)
        else:
            if '"' in f:
                return None
            parsed.append(None if f == "" else f)
    return parsed


def _iter_csv_rows(file_glob: str, expected_cols: int):
    for path in sorted(glob.glob(file_glob)):
        with open(path, "rb") as f:
            lines = f.read().decode("utf-8").split("\n")
        for line in lines[1:]:
            row = parse_strict_csv_line(line, expected_cols)
            if row is not None:
                yield row


def build_small_tables(data_dir: str) -> tuple[pa.Table, pa.Table, pa.Table]:
    """
    Fast Python Union-Find on links/*.csv + small dimension tables (products, campaigns).
    Returns PyArrow tables: (user_map, products, campaigns).
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    def union(a: str, b: str):
        if a not in parent:
            parent[a] = a
        if b not in parent:
            parent[b] = b
        ra, rb = find(a), find(b)
        if ra != rb:
            if ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb

    for uid_a, uid_b in _iter_csv_rows(os.path.join(data_dir, "links", "*.csv"), 2):
        if uid_a is not None and uid_b is not None:
            union(uid_a, uid_b)

    raw_ids = list(parent.keys())
    canon_ids = [find(uid) for uid in raw_ids]
    user_map_tbl = pa.table({
        "raw_user_id": pa.array(raw_ids, type=pa.string()),
        "canonical_user_id": pa.array(canon_ids, type=pa.string()),
    })

    prod_ids, prod_cats = [], []
    prod_path = os.path.join(data_dir, "products.csv")
    if os.path.exists(prod_path):
        for pid, cat in _iter_csv_rows(prod_path, 2):
            if pid is not None and cat is not None:
                prod_ids.append(pid)
                prod_cats.append(cat)
    products_tbl = pa.table({
        "product_id": pa.array(prod_ids, type=pa.string()),
        "category": pa.array(prod_cats, type=pa.string()),
    })

    camp_ids, camp_chans = [], []
    camp_path = os.path.join(data_dir, "campaigns.csv")
    if os.path.exists(camp_path):
        for cid, chan in _iter_csv_rows(camp_path, 2):
            if cid is not None and chan is not None:
                camp_ids.append(cid)
                camp_chans.append(chan)
    campaigns_tbl = pa.table({
        "campaign_id": pa.array(camp_ids, type=pa.string()),
        "channel": pa.array(camp_chans, type=pa.string()),
    })

    return user_map_tbl, products_tbl, campaigns_tbl
