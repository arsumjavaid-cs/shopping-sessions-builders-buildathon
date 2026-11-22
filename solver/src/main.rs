use arrow::array::{Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rayon::prelude::*;
use serde::Deserialize;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use mimalloc::MiMalloc;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use std::hash::{Hash, Hasher};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const SEVEN_DAYS_MS: i64 = 7 * 86_400_000;
const TWENTY_FOUR_HOURS_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000;

#[derive(Deserialize)]
struct Manifest {
    window_start: String,
    window_end: String,
}

#[derive(Clone)]
struct RawEvent {
    user_id: Box<[u8]>,
    ts_str: Box<[u8]>,
    ingest_ms: i64,
    event_type: u8, // 1=view, 2=cart, 3=purchase
    product_id: Box<[u8]>,
    price_str: Box<[u8]>,
}

#[derive(Clone)]
struct ValidEvent {
    ts: i64,
    event_type: u8,
    cat_idx: u32,
    price_cents: i64,
}

#[derive(Clone)]
struct ValidClick {
    ts: i64,
    click_id: Box<[u8]>,
    camp_idx: u32,
}

#[inline(always)]
fn parse_strict_field(f: &[u8]) -> Option<&[u8]> {
    if f.len() >= 2 && f.first() == Some(&b'"') && f.last() == Some(&b'"') {
        let inner = &f[1..f.len() - 1];
        if inner.contains(&b'"') {
            return None;
        }
        Some(inner)
    } else {
        if f.contains(&b'"') {
            return None;
        }
        Some(f)
    }
}

fn split_strict_line<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut idx = 0;
    let mut start = 0;
    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if idx >= N {
                return None;
            }
            out[idx] = parse_strict_field(&line[start..i])?;
            idx += 1;
            start = i + 1;
        }
    }
    if idx != N - 1 {
        return None;
    }
    out[idx] = parse_strict_field(&line[start..])?;
    Some(out)
}

#[inline(always)]
fn parse_ascii_digits_u64(s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let mut val: u64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    Some(val)
}

#[inline(always)]
fn parse_2_digits(s: &[u8]) -> Option<i64> {
    if s.len() != 2 || !s[0].is_ascii_digit() || !s[1].is_ascii_digit() {
        return None;
    }
    Some(((s[0] - b'0') * 10 + (s[1] - b'0')) as i64)
}

#[inline(always)]
fn parse_4_digits(s: &[u8]) -> Option<i64> {
    if s.len() != 4 || !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(
        ((s[0] - b'0') as i64) * 1000
            + ((s[1] - b'0') as i64) * 100
            + ((s[2] - b'0') as i64) * 10
            + ((s[3] - b'0') as i64),
    )
}

#[inline(always)]
fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn parse_ts_ms(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    // F3: digits only
    if s.iter().all(|b| b.is_ascii_digit()) {
        let v = parse_ascii_digits_u64(s)?;
        if v > i64::MAX as u64 {
            return None;
        }
        return Some(v as i64);
    }
    // F1 (len 20) or F2 (len 25)
    if (s.len() != 20 && s.len() != 25)
        || s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
    {
        return None;
    }
    let yr = parse_4_digits(&s[0..4])?;
    let mo = parse_2_digits(&s[5..7])?;
    let dy = parse_2_digits(&s[8..10])?;
    let hr = parse_2_digits(&s[11..13])?;
    let mi = parse_2_digits(&s[14..16])?;
    let sc = parse_2_digits(&s[17..19])?;

    if !(1..=12).contains(&mo) || !(0..=23).contains(&hr) || !(0..=59).contains(&mi) || !(0..=59).contains(&sc) {
        return None;
    }
    let max_d = days_in_month(yr, mo);
    if !(1..=max_d).contains(&dy) {
        return None;
    }

    let mut total_sec = days_from_civil(yr, mo, dy) * 86_400 + hr * 3600 + mi * 60 + sc;

    if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = s[19];
        if (sign != b'+' && sign != b'-') || s[22] != b':' {
            return None;
        }
        let off_h = parse_2_digits(&s[20..22])?;
        let off_m = parse_2_digits(&s[23..25])?;
        if !(0..=23).contains(&off_h) || !(0..=59).contains(&off_m) {
            return None;
        }
        let off_sec = off_h * 3600 + off_m * 60;
        if sign == b'+' {
            total_sec -= off_sec;
        } else {
            total_sec += off_sec;
        }
    }
    Some(total_sec * 1000)
}

fn list_csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("csv") {
                files.push(p);
            }
        }
    }
    files.sort();
    files
}

fn build_user_map(links_dir: &Path) -> FxHashMap<Box<[u8]>, Box<[u8]>> {
    let mut id_to_idx: FxHashMap<Box<[u8]>, usize> = FxHashMap::default();
    let mut idx_to_id: Vec<Box<[u8]>> = Vec::new();
    let mut parent: Vec<usize> = Vec::new();

    fn find(parent: &mut [usize], mut x: usize) -> usize {
        let mut root = x;
        while parent[root] != root {
            root = parent[root];
        }
        while parent[x] != root {
            let nxt = parent[x];
            parent[x] = root;
            x = nxt;
        }
        root
    }

    for path in list_csv_files(links_dir) {
        let bytes = fs::read(&path).unwrap_or_default();
        for line in bytes.split(|&b| b == b'\n').skip(1) {
            if let Some([a, b]) = split_strict_line::<2>(line) {
                if a.is_empty() || b.is_empty() {
                    continue;
                }
                let ia = match id_to_idx.get(a) {
                    Some(&i) => i,
                    None => {
                        let i = idx_to_id.len();
                        let boxed: Box<[u8]> = a.into();
                        id_to_idx.insert(boxed.clone(), i);
                        idx_to_id.push(boxed);
                        parent.push(i);
                        i
                    }
                };
                let ib = match id_to_idx.get(b) {
                    Some(&i) => i,
                    None => {
                        let i = idx_to_id.len();
                        let boxed: Box<[u8]> = b.into();
                        id_to_idx.insert(boxed.clone(), i);
                        idx_to_id.push(boxed);
                        parent.push(i);
                        i
                    }
                };
                let ra = find(&mut parent, ia);
                let rb = find(&mut parent, ib);
                if ra != rb {
                    if idx_to_id[ra] < idx_to_id[rb] {
                        parent[rb] = ra;
                    } else {
                        parent[ra] = rb;
                    }
                }
            }
        }
    }

    let mut map = FxHashMap::default();
    map.reserve(idx_to_id.len());
    for i in 0..idx_to_id.len() {
        let r = find(&mut parent, i);
        map.insert(idx_to_id[i].clone(), idx_to_id[r].clone());
    }
    map
}

fn main() {
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());
    let out_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".to_string());
    let data_path = Path::new(&data_dir);
    let out_path = Path::new(&out_dir);

    let manifest_bytes = fs::read(data_path.join("manifest.json")).expect("manifest.json");
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).expect("parse manifest");
    let win_start = parse_ts_ms(manifest.window_start.as_bytes()).expect("win_start");
    let win_end = parse_ts_ms(manifest.window_end.as_bytes()).expect("win_end");
    let click_win_start = win_start - SEVEN_DAYS_MS;

    // 1. Load dimensions + Union-Find in parallel
            let (user_map, ((prod_map, cat_names), (camp_map, camp_names, camp_channels))) = rayon::join(
        || build_user_map(&data_path.join("links")),
        || {
            rayon::join(
                || {
                    let mut cat_to_idx: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
                    let mut cat_names: Vec<String> = vec!["unknown".to_string()];
                    cat_to_idx.insert(b"unknown".as_slice().into(), 0);
                    let mut prod_map: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();

                    if let Ok(bytes) = fs::read(data_path.join("products.csv")) {
                        for line in bytes.split(|&b| b == b'\n').skip(1) {
                            if let Some([pid, cat]) = split_strict_line::<2>(line) {
                                if !pid.is_empty() && !cat.is_empty() {
                                    let cidx = match cat_to_idx.get(cat) {
                                        Some(&i) => i,
                                        None => {
                                            let i = cat_names.len() as u32;
                                            cat_names.push(String::from_utf8(cat.to_vec()).unwrap());
                                            cat_to_idx.insert(cat.into(), i);
                                            i
                                        }
                                    };
                                    prod_map.insert(pid.into(), cidx);
                                }
                            }
                        }
                    }
                    (prod_map, cat_names)
                },
                || {
                    let mut camp_map: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
                    let mut camp_names: Vec<String> = vec!["organic".to_string()];
                    let mut camp_channels: Vec<String> = vec!["organic".to_string()];
                    camp_map.insert(b"organic".as_slice().into(), 0);

                    if let Ok(bytes) = fs::read(data_path.join("campaigns.csv")) {
                        for line in bytes.split(|&b| b == b'\n').skip(1) {
                            if let Some([cid, chan]) = split_strict_line::<2>(line) {
                                if !cid.is_empty() && !chan.is_empty() {
                                    let idx = camp_names.len() as u32;
                                    camp_names.push(String::from_utf8(cid.to_vec()).unwrap());
                                    camp_channels.push(String::from_utf8(chan.to_vec()).unwrap());
                                    camp_map.insert(cid.into(), idx);
                                }
                            }
                        }
                    }
                    (camp_map, camp_names, camp_channels)
                },
            )
        },
    );

        // 2. Parallel Events Ingestion -> 16-Shard Parallel Dedup & Validation
    let event_files = list_csv_files(&data_path.join("events"));
    let per_file_sharded: Vec<[Vec<(Box<[u8]>, RawEvent)>; 16]> = event_files
        .par_iter()
        .map(|path| {
            let mut local: FxHashMap<Box<[u8]>, RawEvent> = FxHashMap::default();
            let bytes = fs::read(path).unwrap_or_default();
            for line in bytes.split(|&b| b == b'\n').skip(1) {
                if let Some([eid, uid, ts_s, ing_s, etype, pid, price_s]) = split_strict_line::<7>(line) {
                    if eid.is_empty() {
                        continue;
                    }
                    let ingest_ms = match parse_ts_ms(ing_s) {
                        Some(v) => v,
                        None => continue,
                    };
                    if let Some(prev) = local.get(eid) {
                        if ingest_ms >= prev.ingest_ms {
                            continue;
                        }
                    }
                    let et_code = match etype {
                        b"view" => 1,
                        b"cart" => 2,
                        b"purchase" => 3,
                        _ => 0,
                    };
                    local.insert(
                        eid.into(),
                        RawEvent {
                            user_id: uid.into(),
                            ts_str: ts_s.into(),
                            ingest_ms,
                            event_type: et_code,
                            product_id: pid.into(),
                            price_str: price_s.into(),
                        },
                    );
                }
            }
            let mut shards: [Vec<(Box<[u8]>, RawEvent)>; 16] = std::array::from_fn(|_| Vec::new());
            for (eid, ev) in local {
                let mut h = FxHasher::default();
                eid.hash(&mut h);
                let shard_idx = (h.finish() as usize) & 15;
                shards[shard_idx].push((eid, ev));
            }
            shards
        })
        .collect();

    // Process all 16 shards across all CPU cores in parallel (dedup + validate)
    let shard_user_maps: Vec<FxHashMap<Box<[u8]>, Vec<ValidEvent>>> = (0..16usize)
        .into_par_iter()
        .map(|shard_idx| {
            let mut shard_dedup: FxHashMap<Box<[u8]>, RawEvent> = FxHashMap::default();
            for file_shards in &per_file_sharded {
                for (eid, ev) in &file_shards[shard_idx] {
                    match shard_dedup.get_mut(eid) {
                        Some(prev) => {
                            if ev.ingest_ms < prev.ingest_ms {
                                *prev = ev.clone();
                            }
                        }
                        None => {
                            shard_dedup.insert(eid.clone(), ev.clone());
                        }
                    }
                }
            }

            let mut local_users: FxHashMap<Box<[u8]>, Vec<ValidEvent>> = FxHashMap::default();
            for (_, ev) in shard_dedup {
                if ev.user_id.is_empty() || ev.product_id.is_empty() || ev.event_type == 0 {
                    continue;
                }
                let ts = match parse_ts_ms(&ev.ts_str) {
                    Some(v) if v >= win_start && v < win_end => v,
                    _ => continue,
                };
                if ev.ingest_ms - ts > TWENTY_FOUR_HOURS_MS {
                    continue;
                }
                let mut price_cents: i64 = 0;
                if ev.event_type == 3 {
                    let p = match parse_ascii_digits_u64(&ev.price_str) {
                        Some(v) if (1..=10_000_000).contains(&v) => v as i64,
                        _ => continue,
                    };
                    price_cents = p;
                }
                let cat_idx = *prod_map.get(&ev.product_id).unwrap_or(&0);
                let canon_uid = user_map.get(&ev.user_id).cloned().unwrap_or(ev.user_id);
                local_users.entry(canon_uid).or_default().push(ValidEvent {
                    ts,
                    event_type: ev.event_type,
                    cat_idx,
                    price_cents,
                });
            }
            local_users
        })
        .collect();

    let mut user_events: FxHashMap<Box<[u8]>, Vec<ValidEvent>> = FxHashMap::default();
    for map in shard_user_maps {
        for (uid, mut evs) in map {
            user_events.entry(uid).or_default().append(&mut evs);
        }
    }

    // 4. Parallel Clicks Ingestion, Dedup & Validation
    let click_files = list_csv_files(&data_path.join("clicks"));
    let per_file_clicks: Vec<Vec<(Box<[u8]>, Box<[u8]>, u32, i64)>> = click_files
        .par_iter()
        .map(|path| {
            let mut out = Vec::new();
            let bytes = fs::read(path).unwrap_or_default();
            for line in bytes.split(|&b| b == b'\n').skip(1) {
                if let Some([cid, uid, camp_id, ts_s]) = split_strict_line::<4>(line) {
                    if cid.is_empty() || uid.is_empty() || camp_id.is_empty() || camp_id == b"organic" {
                        continue;
                    }
                    let camp_idx = match camp_map.get(camp_id) {
                        Some(&idx) => idx,
                        None => continue,
                    };
                    let ts = match parse_ts_ms(ts_s) {
                        Some(v) if v >= click_win_start && v < win_end => v,
                        _ => continue,
                    };
                    out.push((cid.into(), uid.into(), camp_idx, ts));
                }
            }
            out
        })
        .collect();

    let mut seen_clicks: FxHashSet<Box<[u8]>> = FxHashSet::default();
    let mut user_clicks: FxHashMap<Box<[u8]>, Vec<ValidClick>> = FxHashMap::default();
    for batch in per_file_clicks {
        for (cid, raw_uid, camp_idx, ts) in batch {
            if !seen_clicks.insert(cid.clone()) {
                continue;
            }
            let canon_uid = user_map.get(&raw_uid).cloned().unwrap_or(raw_uid);
            if user_events.contains_key(&canon_uid) {
                user_clicks.entry(canon_uid).or_default().push(ValidClick {
                    ts,
                    click_id: cid,
                    camp_idx,
                });
            }
        }
    }

    user_clicks.par_iter_mut().for_each(|(_, c)| {
    c.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| b.click_id.cmp(&a.click_id)));
});

    // 5. Parallel Sessionization + Attribution + Per-User Aggregation
    let mut users_vec: Vec<(Box<[u8]>, Vec<ValidEvent>)> = user_events.into_iter().collect();

    struct UserDailyRow {
        user_id: String,
        day_epoch_days: i32,
        sessions: i64,
        events: i64,
        revenue_cents: i64,
        top_category: Option<String>,
    }

    type CampMap = FxHashMap<(u32, i32), (i64, i64, i64, i64)>;

    let (user_rows, camp_partial_maps): (Vec<Vec<UserDailyRow>>, Vec<CampMap>) = users_vec
        .par_iter_mut()
        .map(|(uid_bytes, evs)| {
            evs.sort_by_key(|e| e.ts);
            let uid_str = String::from_utf8(uid_bytes.to_vec()).unwrap();

            let clicks: &[ValidClick] = user_clicks.get(uid_bytes).map(|v| v.as_slice()).unwrap_or(&[]);

            let mut day_user_stats: FxHashMap<i32, (i64, i64, i64, FxHashMap<u32, i64>)> = FxHashMap::default();
            let mut local_camp: CampMap = FxHashMap::default();

            let mut i = 0;
            while i < evs.len() {
                let sess_start = evs[i].ts;
                let sess_day = sess_start.div_euclid(86_400_000) as i32;
                let mut sess_events: i64 = 0;
                let mut sess_rev: i64 = 0;
                let mut prev_ts = sess_start;

                let u_entry = day_user_stats
                    .entry(sess_day)
                    .or_insert_with(|| (0, 0, 0, FxHashMap::default()));
                u_entry.0 += 1;

                while i < evs.len() {
                    let e = &evs[i];
                    if e.ts - prev_ts > SESSION_GAP_MS {
                        break;
                    }
                    prev_ts = e.ts;
                    sess_events += 1;
                    if e.event_type == 3 {
                        sess_rev += e.price_cents;
                        *u_entry.3.entry(e.cat_idx).or_insert(0) += e.price_cents;
                    }
                    i += 1;
                }
                u_entry.1 += sess_events;
                u_entry.2 += sess_rev;

                let min_click_ts = sess_start - SEVEN_DAYS_MS;
                let lo = clicks.partition_point(|c| c.ts < min_click_ts);
                let hi = clicks.partition_point(|c| c.ts <= sess_start);

                if lo < hi {
                    let best_camp = clicks[hi - 1].camp_idx;
                    let c_entry = local_camp.entry((best_camp, sess_day)).or_insert((0, 0, 0, 0));
                    c_entry.0 += 1;
                    if sess_rev > 0 {
                        c_entry.1 += 1;
                    }
                    c_entry.2 += sess_rev;

                    let mut seen_assist: FxHashSet<u32> = FxHashSet::default();
                    for c in &clicks[lo..hi] {
                        if seen_assist.insert(c.camp_idx) {
                            local_camp.entry((c.camp_idx, sess_day)).or_insert((0, 0, 0, 0)).3 += 1;
                        }
                    }
                } else {
                    let c_entry = local_camp.entry((0, sess_day)).or_insert((0, 0, 0, 0));
                    c_entry.0 += 1;
                    if sess_rev > 0 {
                        c_entry.1 += 1;
                    }
                    c_entry.2 += sess_rev;
                }
            }

            let mut u_rows = Vec::with_capacity(day_user_stats.len());
            for (day, (sessions, events, rev, cat_map)) in day_user_stats {
                let top_cat = if rev > 0 {
                    let mut best_name: Option<&String> = None;
                    let mut best_val: i64 = -1;
                    for (cidx, cval) in cat_map {
                        let cname = &cat_names[cidx as usize];
                        if cval > best_val || (cval == best_val && best_name.map_or(true, |bn| cname < bn)) {
                            best_val = cval;
                            best_name = Some(cname);
                        }
                    }
                    best_name.cloned()
                } else {
                    None
                };
                u_rows.push(UserDailyRow {
                    user_id: uid_str.clone(),
                    day_epoch_days: day,
                    sessions,
                    events,
                    revenue_cents: rev,
                    top_category: top_cat,
                });
            }

            (u_rows, local_camp)
        })
        .unzip();

    // 6. Merge campaign stats & write Parquet files
    let mut global_camp: CampMap = FxHashMap::default();
    for m in camp_partial_maps {
        for (k, v) in m {
            let e = global_camp.entry(k).or_insert((0, 0, 0, 0));
            e.0 += v.0;
            e.1 += v.1;
            e.2 += v.2;
            e.3 += v.3;
        }
    }

    fs::create_dir_all(out_path.join("user_daily")).expect("mkdir user_daily");
    fs::create_dir_all(out_path.join("campaign_daily")).expect("mkdir campaign_daily");

    let total_u_rows: usize = user_rows.iter().map(|v| v.len()).sum();
    let mut u_ids = Vec::with_capacity(total_u_rows);
    let mut u_days = Vec::with_capacity(total_u_rows);
    let mut u_sess = Vec::with_capacity(total_u_rows);
    let mut u_evs = Vec::with_capacity(total_u_rows);
    let mut u_rev = Vec::with_capacity(total_u_rows);
    let mut u_top = Vec::with_capacity(total_u_rows);

    for batch in user_rows {
        for r in batch {
            u_ids.push(r.user_id);
            u_days.push(r.day_epoch_days);
            u_sess.push(r.sessions);
            u_evs.push(r.events);
            u_rev.push(r.revenue_cents);
            u_top.push(r.top_category);
        }
    }

    let u_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let u_batch = RecordBatch::try_new(
        u_schema.clone(),
        vec![
            Arc::new(StringArray::from(u_ids)),
            Arc::new(Date32Array::from(u_days)),
            Arc::new(Int64Array::from(u_sess)),
            Arc::new(Int64Array::from(u_evs)),
            Arc::new(Int64Array::from(u_rev)),
            Arc::new(StringArray::from(u_top)),
        ],
    )
    .unwrap();
    let u_file = File::create(out_path.join("user_daily").join("part-0.parquet")).unwrap();
    let mut u_writer = ArrowWriter::try_new(u_file, u_schema, None).unwrap();
    u_writer.write(&u_batch).unwrap();
    u_writer.close().unwrap();

    let total_c_rows = global_camp.len();
    let mut c_ids = Vec::with_capacity(total_c_rows);
    let mut c_chans = Vec::with_capacity(total_c_rows);
    let mut c_days = Vec::with_capacity(total_c_rows);
    let mut c_sess = Vec::with_capacity(total_c_rows);
    let mut c_psess = Vec::with_capacity(total_c_rows);
    let mut c_rev = Vec::with_capacity(total_c_rows);
    let mut c_asess = Vec::with_capacity(total_c_rows);

    for ((camp_idx, day), (sess, psess, rev, asess)) in global_camp {
        c_ids.push(camp_names[camp_idx as usize].clone());
        c_chans.push(camp_channels[camp_idx as usize].clone());
        c_days.push(day);
        c_sess.push(sess);
        c_psess.push(psess);
        c_rev.push(rev);
        c_asess.push(asess);
    }

    let c_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let c_batch = RecordBatch::try_new(
        c_schema.clone(),
        vec![
            Arc::new(StringArray::from(c_ids)),
            Arc::new(StringArray::from(c_chans)),
            Arc::new(Date32Array::from(c_days)),
            Arc::new(Int64Array::from(c_sess)),
            Arc::new(Int64Array::from(c_psess)),
            Arc::new(Int64Array::from(c_rev)),
            Arc::new(Int64Array::from(c_asess)),
        ],
    )
    .unwrap();
    let c_file = File::create(out_path.join("campaign_daily").join("part-0.parquet")).unwrap();
    let mut c_writer = ArrowWriter::try_new(c_file, c_schema, None).unwrap();
    c_writer.write(&c_batch).unwrap();
    c_writer.close().unwrap();
}
