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

SEVEN_DAYS_MS = 7 * 86_400_000
TWENTY_FOUR_HOURS_MS = 86_400_000


def parse_ts_ms(val: str | None) -> int | None:
    if not val:
        return None

    if val.isascii() and val.isdigit() and _DIGITS_RE.match(val):
        try:
            ms = int(val)
            if ms > 9_223_372_036_854_775_807:
                return None
            return ms
        except ValueError:
            return None

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
            1 <= mo <= 12
            and 0 <= hr <= 23
            and 0 <= mi <= 59
            and 0 <= sc <= 59
            and 0 <= off_h <= 23
            and 0 <= off_m <= 59
        ):
            return None
        _, max_day = calendar.monthrange(yr, mo)
        if not (1 <= dy <= max_day):
            return None
        base_sec = calendar.timegm((yr, mo, dy, hr, mi, sc))
        offset_sec = off_h * 3600 + off_m * 60
        if sign == "+":
            base_sec -= offset_sec
        else:
            base_sec += offset_sec
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
            raw_bytes = f.read()
        lines = raw_bytes.decode("utf-8").split("\n")
        for line in lines[1:]:
            row = parse_strict_csv_line(line, expected_cols)
            if row is not None:
                yield row


def build_identity_resolver(links_glob: str):
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

    for uid_a, uid_b in _iter_csv_rows(links_glob, 2):
        if uid_a is not None and uid_b is not None:
            union(uid_a, uid_b)

    resolved_map = {uid: find(uid) for uid in parent}
    return lambda uid: resolved_map.get(uid, uid)


def load_dimensions(data_dir: str) -> tuple[dict[str, str], dict[str, str]]:
    products: dict[str, str] = {}
    prod_path = os.path.join(data_dir, "products.csv")
    if os.path.exists(prod_path):
        for pid, cat in _iter_csv_rows(prod_path, 2):
            if pid is not None and cat is not None:
                products[pid] = cat

    campaigns: dict[str, str] = {}
    camp_path = os.path.join(data_dir, "campaigns.csv")
    if os.path.exists(camp_path):
        for cid, chan in _iter_csv_rows(camp_path, 2):
            if cid is not None and chan is not None:
                campaigns[cid] = chan

    return products, campaigns


def ingest_all(data_dir: str, manifest: dict) -> tuple[pa.Table, pa.Table, pa.Table]:
    win_start = parse_ts_ms(manifest["window_start"])
    win_end = parse_ts_ms(manifest["window_end"])
    assert win_start is not None and win_end is not None
    click_win_start = win_start - SEVEN_DAYS_MS

    resolve_user = build_identity_resolver(os.path.join(data_dir, "links", "*.csv"))
    products, campaigns = load_dimensions(data_dir)

    # --- EVENTS: Step 1 (Parse) & Step 2 (Dedup on event_id BEFORE validation) ---
    best_events: dict[str, tuple[int, list[str | None]]] = {}
    for row in _iter_csv_rows(os.path.join(data_dir, "events", "*.csv"), 7):
        event_id, _, _, ingest_str, _, _, _ = row
        if event_id is None:
            continue
        ingest_ms = parse_ts_ms(ingest_str)
        if ingest_ms is None:
            continue
        prev = best_events.get(event_id)
        if prev is None:
            best_events[event_id] = (ingest_ms, row)
        else:
            prev_ingest_ms, _ = prev
            if ingest_ms < prev_ingest_ms:
                best_events[event_id] = (ingest_ms, row)

    # --- EVENTS: Step 3 (Validate kept copy), Step 4 (Drop late), Step 7 (Category) ---
    ev_users: list[str] = []
    ev_ts: list[int] = []
    ev_types: list[str] = []
    ev_cats: list[str] = []
    ev_prices: list[int] = []

    for ingest_ms, row in best_events.values():
        _, user_id, ts_str, _, event_type, product_id, price_str = row
        if user_id is None or product_id is None:
            continue
        if event_type not in ("view", "cart", "purchase"):
            continue

        ts_ms = parse_ts_ms(ts_str)
        if ts_ms is None or not (win_start <= ts_ms < win_end):
            continue

        if ingest_ms - ts_ms > TWENTY_FOUR_HOURS_MS:
            continue

        price_cents = 0
        if event_type == "purchase":
            if not price_str or not _DIGITS_RE.match(price_str):
                continue
            price_cents = int(price_str)
            if not (1 <= price_cents <= 10_000_000):
                continue

        ev_users.append(resolve_user(user_id))
        ev_ts.append(ts_ms)
        ev_types.append(event_type)
        ev_cats.append(products.get(product_id, "unknown"))
        ev_prices.append(price_cents)

    events_tbl = pa.table({
        "user_id": pa.array(ev_users, type=pa.string()),
        "ts": pa.array(ev_ts, type=pa.int64()),
        "event_type": pa.array(ev_types, type=pa.string()),
        "category": pa.array(ev_cats, type=pa.string()),
        "price_cents": pa.array(ev_prices, type=pa.int64()),
    })

    # --- CLICKS: Step 1 (Parse), Step 2 (Dedup on click_id), Step 3 (Validate) ---
    seen_clicks: set[str] = set()
    cl_ids: list[str] = []
    cl_users: list[str] = []
    cl_camps: list[str] = []
    cl_ts: list[int] = []

    for row in _iter_csv_rows(os.path.join(data_dir, "clicks", "*.csv"), 4):
        click_id, user_id, campaign_id, ts_str = row
        if click_id is None or click_id in seen_clicks:
            continue
        seen_clicks.add(click_id)

        if user_id is None or campaign_id is None or campaign_id not in campaigns:
            continue
        ts_ms = parse_ts_ms(ts_str)
        if ts_ms is None or not (click_win_start <= ts_ms < win_end):
            continue

        cl_ids.append(click_id)
        cl_users.append(resolve_user(user_id))
        cl_camps.append(campaign_id)
        cl_ts.append(ts_ms)

    clicks_tbl = pa.table({
        "click_id": pa.array(cl_ids, type=pa.string()),
        "user_id": pa.array(cl_users, type=pa.string()),
        "campaign_id": pa.array(cl_camps, type=pa.string()),
        "ts": pa.array(cl_ts, type=pa.int64()),
    })

    campaigns_tbl = pa.table({
        "campaign_id": pa.array(list(campaigns.keys()), type=pa.string()),
        "channel": pa.array(list(campaigns.values()), type=pa.string()),
    })

    return events_tbl, clicks_tbl, campaigns_tbl
