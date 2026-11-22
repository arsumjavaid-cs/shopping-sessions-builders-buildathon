use arrow::array::{ArrayRef, Date32Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use memchr::{memchr, memchr_iter};
use mimalloc::MiMalloc;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use std::fs::{self, File};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const DAY_MS: i64 = 86_400_000;
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const TWENTY_FOUR_HOURS_MS: i64 = DAY_MS;
const SESSION_GAP_MS: i64 = 1_800_000;

/// Interner shards (power of two). A raw id is `(local_index << 8) | shard`.
const NSHARD: usize = 256;
/// Event-dedup buckets (by event-id hash) and person buckets (by person id).
const NB: usize = 256;
const NPB: usize = 256;
/// Bytes per streamed chunk of an events file / per in-memory chunk of small files.
const EVENT_CHUNK: u64 = 8 << 20;
const MEM_CHUNK: usize = 4 << 20;
const NPARTS: usize = 16;

#[derive(Deserialize)]
struct Manifest {
    window_start: String,
    window_end: String,
}

#[inline(always)]
fn parse_strict_field(f: &[u8]) -> Option<&[u8]> {
    if f.len() >= 2 && f.first() == Some(&b'"') && f.last() == Some(&b'"') {
        let inner = &f[1..f.len() - 1];
        if memchr(b'"', inner).is_some() {
            return None;
        }
        Some(inner)
    } else {
        if memchr(b'"', f).is_some() {
            return None;
        }
        Some(f)
    }
}

#[inline(always)]
fn split_strict_line<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0;
    for slot in out.iter_mut().take(N - 1) {
        let rel = memchr(b',', &line[start..])?;
        *slot = parse_strict_field(&line[start..start + rel])?;
        start += rel + 1;
    }
    if memchr(b',', &line[start..]).is_some() {
        return None;
    }
    out[N - 1] = parse_strict_field(&line[start..])?;
    Some(out)
}

#[inline(always)]
fn parse_ascii_digits_u64(s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let mut val: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val.checked_mul(10)?.checked_add(d as u64)?;
    }
    Some(val)
}

#[inline(always)]
fn parse_2_digits(s: &[u8]) -> Option<i64> {
    let &[a, b] = s else {
        return None;
    };
    let da = a.wrapping_sub(b'0');
    let db = b.wrapping_sub(b'0');
    if da > 9 || db > 9 {
        return None;
    }
    Some((da * 10 + db) as i64)
}

#[inline(always)]
fn parse_4_digits(s: &[u8]) -> Option<i64> {
    let &[a, b, c, d] = s else {
        return None;
    };
    let da = a.wrapping_sub(b'0');
    let db = b.wrapping_sub(b'0');
    let dc = c.wrapping_sub(b'0');
    let dd = d.wrapping_sub(b'0');
    if da > 9 || db > 9 || dc > 9 || dd > 9 {
        return None;
    }
    Some((da as i64) * 1000 + (db as i64) * 100 + (dc as i64) * 10 + (dd as i64))
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
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn parse_ts_ms(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.first().unwrap().is_ascii_digit() && s.len() != 20 && s.len() != 25 {
        let v = parse_ascii_digits_u64(s)?;
        if v > i64::MAX as u64 {
            return None;
        }
        return Some(v as i64);
    }

    let (yr_s, mo_s, dy_s, hr_s, mi_s, sc_s, tail) = match s {
        [y1, y2, y3, y4, b'-', m1, m2, b'-', d1, d2, b'T', h1, h2, b':', min1, min2, b':', s1, s2, rest @ ..] => {
            (
                [*y1, *y2, *y3, *y4],
                [*m1, *m2],
                [*d1, *d2],
                [*h1, *h2],
                [*min1, *min2],
                [*s1, *s2],
                rest,
            )
        }
        _ => {
            let v = parse_ascii_digits_u64(s)?;
            if v > i64::MAX as u64 {
                return None;
            }
            return Some(v as i64);
        }
    };

    let yr = parse_4_digits(&yr_s)?;
    let mo = parse_2_digits(&mo_s)?;
    let dy = parse_2_digits(&dy_s)?;
    let hr = parse_2_digits(&hr_s)?;
    let mi = parse_2_digits(&mi_s)?;
    let sc = parse_2_digits(&sc_s)?;

    if !(1..=12).contains(&mo)
        || !(0..=23).contains(&hr)
        || !(0..=59).contains(&mi)
        || !(0..=59).contains(&sc)
    {
        return None;
    }
    let max_d = days_in_month(yr, mo);
    if !(1..=max_d).contains(&dy) {
        return None;
    }

    let mut total_sec = days_from_civil(yr, mo, dy) * 86_400 + hr * 3600 + mi * 60 + sc;

    match tail {
        [b'Z'] => {}
        [sign @ (b'+' | b'-'), oh1, oh2, b':', om1, om2] => {
            let off_h = parse_2_digits(&[*oh1, *oh2])?;
            let off_m = parse_2_digits(&[*om1, *om2])?;
            if !(0..=23).contains(&off_h) || !(0..=59).contains(&off_m) {
                return None;
            }
            let off_sec = off_h * 3600 + off_m * 60;
            if *sign == b'+' {
                total_sec -= off_sec;
            } else {
                total_sec += off_sec;
            }
        }
        _ => return None,
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


// ---------------------------------------------------------------------------
// Line / chunk iteration
// ---------------------------------------------------------------------------

#[inline(always)]
fn for_each_line<'a, F: FnMut(&'a [u8])>(body: &'a [u8], mut f: F) {
    let mut start = 0;
    for nl in memchr_iter(b'\n', body) {
        f(&body[start..nl]);
        start = nl + 1;
    }
    if start < body.len() {
        f(&body[start..]);
    }
}

/// Splits an in-memory CSV file (header first) into line-aligned body ranges.
fn chunk_ranges(bytes: &[u8], target: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = match memchr(b'\n', bytes) {
        Some(i) => i + 1,
        None => return out,
    };
    while start < bytes.len() {
        let mut end = (start + target).min(bytes.len());
        if end < bytes.len() {
            let probe = end - 1;
            end = match memchr(b'\n', &bytes[probe..]) {
                Some(i) => probe + i + 1,
                None => bytes.len(),
            };
        }
        out.push((start, end));
        start = end;
    }
    out
}

/// Reads the lines whose first byte lies in [start, end) of a file, completing the last
/// line. Returns the buffer and the offset of the first owned line inside it. The header
/// line (owned by the chunk starting at 0) is skipped.
fn read_chunk(file: &File, len: u64, start: u64, end: u64) -> (Vec<u8>, usize) {
    let lead = if start > 0 { 1u64 } else { 0 };
    let rs = start - lead;
    let mut buf = vec![0u8; (end - rs) as usize];
    file.read_exact_at(&mut buf, rs).expect("read events chunk");
    if end < len && buf.last() != Some(&b'\n') {
        let mut off = end;
        let mut tmp = vec![0u8; 64 * 1024];
        while off < len {
            let want = ((len - off) as usize).min(tmp.len());
            file.read_exact_at(&mut tmp[..want], off)
                .expect("read events tail");
            if let Some(i) = memchr(b'\n', &tmp[..want]) {
                buf.extend_from_slice(&tmp[..=i]);
                break;
            }
            buf.extend_from_slice(&tmp[..want]);
            off += want as u64;
        }
    }
    let body_start = memchr(b'\n', &buf).map_or(buf.len(), |i| i + 1);
    (buf, body_start)
}

// ---------------------------------------------------------------------------
// ID interner (sharded, batch-locked)
// ---------------------------------------------------------------------------

struct Shard {
    map: FxHashMap<Arc<[u8]>, u32>,
    names: Vec<Arc<[u8]>>,
}

struct Interner {
    shards: Vec<Mutex<Shard>>,
}

impl Interner {
    fn new() -> Self {
        Interner {
            shards: (0..NSHARD)
                .map(|_| {
                    Mutex::new(Shard {
                        map: FxHashMap::default(),
                        names: Vec::new(),
                    })
                })
                .collect(),
        }
    }
}

/// Resolves every id to its raw id, locking each shard once per call.
fn resolve_ids(it: &Interner, ids: &[&[u8]], out: &mut [u32]) {
    let n = ids.len();
    if n == 0 {
        return;
    }
    let mut sh: Vec<u8> = Vec::with_capacity(n);
    let mut start = [0u32; NSHARD + 1];
    for b in ids {
        let s = (xxh3_64(b) as usize) & (NSHARD - 1);
        sh.push(s as u8);
        start[s + 1] += 1;
    }
    for s in 0..NSHARD {
        start[s + 1] += start[s];
    }
    let mut pos = start;
    let mut order = vec![0u32; n];
    for (i, &s) in sh.iter().enumerate() {
        let p = &mut pos[s as usize];
        order[*p as usize] = i as u32;
        *p += 1;
    }
    for s in 0..NSHARD {
        let (a, b) = (start[s] as usize, start[s + 1] as usize);
        if a == b {
            continue;
        }
        let mut g = it.shards[s].lock().unwrap();
        for &i in &order[a..b] {
            let key = ids[i as usize];
            let raw = match g.map.get(key) {
                Some(&r) => r,
                None => {
                    let raw = ((g.names.len() as u32) << 8) | s as u32;
                    let arc: Arc<[u8]> = Arc::from(key);
                    g.names.push(arc.clone());
                    g.map.insert(arc, raw);
                    raw
                }
            };
            out[i as usize] = raw;
        }
    }
}

fn name_of<'a>(g: &'a [MutexGuard<'_, Shard>], raw: u32) -> &'a [u8] {
    &g[(raw as usize) & (NSHARD - 1)].names[(raw >> 8) as usize]
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    let mut root = x;
    while parent[root as usize] != root {
        root = parent[root as usize];
    }
    while parent[x as usize] != root {
        let nx = parent[x as usize];
        parent[x as usize] = root;
        x = nx;
    }
    root
}

/// Parses the links files, joins linked ids and returns `person_of[raw]`: the raw id of the
/// byte-wise smallest id of the person.
fn build_person_map(interner: &Interner, link_bufs: &[Vec<u8>]) -> Vec<u32> {
    let mut ranges = Vec::new();
    for (fi, b) in link_bufs.iter().enumerate() {
        for (s, e) in chunk_ranges(b, MEM_CHUNK) {
            ranges.push((fi, s, e));
        }
    }
    let pairs: Vec<Vec<(u32, u32)>> = ranges
        .par_iter()
        .map(|&(fi, s, e)| {
            let body = &link_bufs[fi][s..e];
            let mut ids: Vec<&[u8]> = Vec::new();
            for_each_line(body, |line| {
                if let Some([a, b]) = split_strict_line::<2>(line) {
                    if !a.is_empty() && !b.is_empty() {
                        ids.push(a);
                        ids.push(b);
                    }
                }
            });
            let mut raws = vec![0u32; ids.len()];
            resolve_ids(interner, &ids, &mut raws);
            raws.chunks_exact(2).map(|c| (c[0], c[1])).collect()
        })
        .collect();

    let guards: Vec<MutexGuard<'_, Shard>> =
        interner.shards.iter().map(|m| m.lock().unwrap()).collect();
    let max_local = guards.iter().map(|g| g.names.len()).max().unwrap_or(0);
    let size = max_local * NSHARD;
    let mut parent: Vec<u32> = (0..size as u32).collect();
    for chunk in &pairs {
        for &(a, b) in chunk {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                if name_of(&guards, ra) < name_of(&guards, rb) {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    (0..size as u32).map(|i| find(&mut parent, i)).collect()
}

#[inline(always)]
fn person(of: &[u32], raw: u32) -> u32 {
    match of.get(raw as usize) {
        Some(&p) => p,
        None => raw,
    }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// A well-formed event line (valid or not): invalid ones still take part in dedup.
#[derive(Clone, Copy)]
struct Rec {
    h_hi: u64,
    h_lo: u64,
    ingest: i64,
    ts: i64,
    pid: u32,
    price: u32,
    cat: u32,
    valid: u32,
}

#[derive(Clone, Copy)]
struct EvOut {
    ts: i64,
    pid: u32,
    price: u32, // 0 for non-purchases
    cat: u32,
}

#[derive(Clone, Copy)]
struct ClickRec<'a> {
    ts: i64,
    pid: u32,
    camp: u32,
    id: &'a [u8],
}

struct EvCtx<'a> {
    win_start: i64,
    win_end: i64,
    prod_map: &'a FxHashMap<Box<[u8]>, u32>,
    interner: &'a Interner,
    person_of: &'a [u32],
    buckets: &'a [Mutex<Vec<Vec<Rec>>>],
}

fn process_event_chunk(ctx: &EvCtx, body: &[u8]) {
    let mut recs: Vec<Rec> = Vec::with_capacity(body.len() / 60 + 16);
    let mut uids: Vec<&[u8]> = Vec::new();
    let mut uid_rec: Vec<u32> = Vec::new();
    for_each_line(body, |line| {
        let Some([eid, uid, ts_s, ing_s, etype, pid, price_s]) = split_strict_line::<7>(line)
        else {
            return;
        };
        if eid.is_empty() {
            return;
        }
        let Some(ingest) = parse_ts_ms(ing_s) else {
            return;
        };
        let h = xxh3_128(eid);
        let mut rec = Rec {
            h_hi: (h >> 64) as u64,
            h_lo: h as u64,
            ingest,
            ts: 0,
            pid: 0,
            price: 0,
            cat: 0,
            valid: 0,
        };
        let et: u8 = match etype {
            b"view" => 1,
            b"cart" => 2,
            b"purchase" => 3,
            _ => 0,
        };
        'v: {
            if uid.is_empty() || pid.is_empty() || et == 0 {
                break 'v;
            }
            let ts = match parse_ts_ms(ts_s) {
                Some(v) if v >= ctx.win_start && v < ctx.win_end => v,
                _ => break 'v,
            };
            if ingest - ts > TWENTY_FOUR_HOURS_MS {
                break 'v;
            }
            if et == 3 {
                let p = match parse_ascii_digits_u64(price_s) {
                    Some(v) if (1..=10_000_000).contains(&v) => v as u32,
                    _ => break 'v,
                };
                rec.price = p;
                rec.cat = *ctx.prod_map.get(pid).unwrap_or(&0);
            }
            rec.ts = ts;
            rec.valid = 1;
            uids.push(uid);
            uid_rec.push(recs.len() as u32);
        }
        recs.push(rec);
    });

    let mut raws = vec![0u32; uids.len()];
    resolve_ids(ctx.interner, &uids, &mut raws);
    for (k, &ri) in uid_rec.iter().enumerate() {
        recs[ri as usize].pid = person(ctx.person_of, raws[k]);
    }

    let mut counts = [0usize; NB];
    for r in &recs {
        counts[(r.h_lo as usize) & (NB - 1)] += 1;
    }
    let mut outv: Vec<Vec<Rec>> = counts.iter().map(|&c| Vec::with_capacity(c)).collect();
    for r in recs {
        outv[(r.h_lo as usize) & (NB - 1)].push(r);
    }
    for (b, v) in outv.into_iter().enumerate() {
        if !v.is_empty() {
            ctx.buckets[b].lock().unwrap().push(v);
        }
    }
}

/// Dedups one hash bucket (keeping the smallest ingest_ts per event_id), drops invalid kept
/// copies and scatters the survivors into person buckets.
fn dedup_bucket(
    b: usize,
    ev_buckets: &[Mutex<Vec<Vec<Rec>>>],
    person_buckets: &[Mutex<Vec<Vec<EvOut>>>],
) {
    let parts = std::mem::take(&mut *ev_buckets[b].lock().unwrap());
    let total: usize = parts.iter().map(|p| p.len()).sum();
    if total == 0 {
        return;
    }
    let mut recs: Vec<Rec> = Vec::with_capacity(total);
    for p in parts {
        recs.extend_from_slice(&p);
    }
    let cap = (total * 2).next_power_of_two().max(16);
    let mask = cap - 1;
    let mut keys = vec![0u64; cap];
    let mut idxs = vec![u32::MAX; cap];
    for i in 0..recs.len() {
        let (hh, hl, ing) = (recs[i].h_hi, recs[i].h_lo, recs[i].ingest);
        let mut s = (hh as usize) & mask;
        loop {
            let j = idxs[s];
            if j == u32::MAX {
                keys[s] = hh;
                idxs[s] = i as u32;
                break;
            }
            if keys[s] == hh && recs[j as usize].h_lo == hl {
                if ing < recs[j as usize].ingest {
                    idxs[s] = i as u32;
                }
                break;
            }
            s = (s + 1) & mask;
        }
    }
    let mut local: Vec<Vec<EvOut>> = (0..NPB).map(|_| Vec::new()).collect();
    for &j in &idxs {
        if j == u32::MAX {
            continue;
        }
        let r = &recs[j as usize];
        if r.valid != 0 {
            local[(r.pid as usize) & (NPB - 1)].push(EvOut {
                ts: r.ts,
                pid: r.pid,
                price: r.price,
                cat: r.cat,
            });
        }
    }
    for (pb, v) in local.into_iter().enumerate() {
        if !v.is_empty() {
            person_buckets[pb].lock().unwrap().push(v);
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

struct CampAcc {
    n_days: usize,
    dense: bool,
    cells: Vec<[i64; 4]>,
    sparse: FxHashMap<(u32, i64), [i64; 4]>,
    stamp: Vec<u32>,
    sid: u32,
}

impl CampAcc {
    fn new(n_camp: usize, n_days: usize) -> Self {
        let dense = n_camp.saturating_mul(n_days) <= 4_000_000;
        CampAcc {
            n_days,
            dense,
            cells: if dense {
                vec![[0; 4]; n_camp * n_days]
            } else {
                Vec::new()
            },
            sparse: FxHashMap::default(),
            stamp: vec![0; n_camp],
            sid: 0,
        }
    }
    #[inline(always)]
    fn cell(&mut self, camp: u32, day_idx: i64) -> &mut [i64; 4] {
        if self.dense {
            &mut self.cells[camp as usize * self.n_days + day_idx as usize]
        } else {
            self.sparse.entry((camp, day_idx)).or_insert([0; 4])
        }
    }
    fn merge(&mut self, o: CampAcc) {
        if self.dense {
            for (a, b) in self.cells.iter_mut().zip(&o.cells) {
                for k in 0..4 {
                    a[k] += b[k];
                }
            }
        } else {
            for (k, v) in o.sparse {
                let c = self.sparse.entry(k).or_insert([0; 4]);
                for i in 0..4 {
                    c[i] += v[i];
                }
            }
        }
    }
}

#[derive(Default)]
struct UserCols {
    pid: Vec<u32>,
    day: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    rev: Vec<i64>,
    cat: Vec<u32>, // u32::MAX = null
}

struct AggCtx<'a> {
    cat_rank: &'a [u32],
    ws_day: i64,
}

fn process_person(
    ev: &[EvOut],
    clicks: &[ClickRec],
    cx: &AggCtx,
    acc: &mut CampAcc,
    cols: &mut UserCols,
    catsum: &mut Vec<(u32, i64)>,
) {
    let pid = ev[0].pid;
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let (mut d_sess, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);

    let flush = |cols: &mut UserCols, catsum: &mut Vec<(u32, i64)>, day: i64, s: i64, e: i64, r: i64| {
        let mut top = u32::MAX;
        if r > 0 {
            let mut best_sum = -1i64;
            for &(c, sum) in catsum.iter() {
                if sum > best_sum
                    || (sum == best_sum && cx.cat_rank[c as usize] < cx.cat_rank[top as usize])
                {
                    best_sum = sum;
                    top = c;
                }
            }
        }
        cols.pid.push(pid);
        cols.day.push(day as i32);
        cols.sessions.push(s);
        cols.events.push(e);
        cols.rev.push(r);
        cols.cat.push(top);
        catsum.clear();
    };

    let mut k = 0;
    while k < ev.len() {
        let start = ev[k].ts;
        let day = start.div_euclid(DAY_MS);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush(cols, catsum, cur_day, d_sess, d_ev, d_rev);
            }
            cur_day = day;
            d_sess = 0;
            d_ev = 0;
            d_rev = 0;
        }
        let mut n = 0i64;
        let mut rev = 0i64;
        let mut prev = start;
        while k < ev.len() {
            let e = &ev[k];
            if e.ts - prev > SESSION_GAP_MS {
                break;
            }
            prev = e.ts;
            n += 1;
            if e.price > 0 {
                rev += e.price as i64;
                match catsum.iter_mut().find(|x| x.0 == e.cat) {
                    Some(x) => x.1 += e.price as i64,
                    None => catsum.push((e.cat, e.price as i64)),
                }
            }
            k += 1;
        }
        d_sess += 1;
        d_ev += n;
        d_rev += rev;

        let min_ts = start - SEVEN_DAYS_MS;
        while lo < clicks.len() && clicks[lo].ts < min_ts {
            lo += 1;
        }
        while hi < clicks.len() && clicks[hi].ts <= start {
            hi += 1;
        }
        let day_idx = day - cx.ws_day;
        if lo < hi {
            let best = clicks[hi - 1].camp;
            let c = acc.cell(best, day_idx);
            c[0] += 1;
            if rev > 0 {
                c[1] += 1;
            }
            c[2] += rev;
            acc.sid += 1;
            let sid = acc.sid;
            for c in &clicks[lo..hi] {
                if acc.stamp[c.camp as usize] != sid {
                    acc.stamp[c.camp as usize] = sid;
                    acc.cell(c.camp, day_idx)[3] += 1;
                }
            }
        } else {
            let c = acc.cell(0, day_idx);
            c[0] += 1;
            if rev > 0 {
                c[1] += 1;
            }
            c[2] += rev;
        }
    }
    flush(cols, catsum, cur_day, d_sess, d_ev, d_rev);
}

fn process_person_bucket<'a>(
    pb: usize,
    person_buckets: &[Mutex<Vec<Vec<EvOut>>>],
    click_buckets: &[Mutex<Vec<Vec<ClickRec<'a>>>>],
    cx: &AggCtx,
    acc: &mut CampAcc,
) -> UserCols {
    let eparts = std::mem::take(&mut *person_buckets[pb].lock().unwrap());
    let cparts = std::mem::take(&mut *click_buckets[pb].lock().unwrap());
    let mut cols = UserCols::default();
    let etotal: usize = eparts.iter().map(|p| p.len()).sum();
    if etotal == 0 {
        return cols;
    }
    let mut evs: Vec<EvOut> = Vec::with_capacity(etotal);
    for p in eparts {
        evs.extend_from_slice(&p);
    }
    evs.sort_unstable_by_key(|e| (e.pid, e.ts));
    let mut clicks: Vec<ClickRec<'a>> =
        Vec::with_capacity(cparts.iter().map(|p| p.len()).sum());
    for p in cparts {
        clicks.extend_from_slice(&p);
    }
    clicks.sort_unstable_by(|a, b| {
        a.pid
            .cmp(&b.pid)
            .then(a.ts.cmp(&b.ts))
            .then_with(|| b.id.cmp(a.id))
    });
    clicks.dedup_by(|cur, prev| cur.pid == prev.pid && cur.ts == prev.ts && cur.id == prev.id);

    let mut catsum: Vec<(u32, i64)> = Vec::new();
    let mut cj = 0usize;
    let mut i = 0usize;
    while i < evs.len() {
        let pid = evs[i].pid;
        let mut j = i + 1;
        while j < evs.len() && evs[j].pid == pid {
            j += 1;
        }
        while cj < clicks.len() && clicks[cj].pid < pid {
            cj += 1;
        }
        let mut ce = cj;
        while ce < clicks.len() && clicks[ce].pid == pid {
            ce += 1;
        }
        process_person(&evs[i..j], &clicks[cj..ce], cx, acc, &mut cols, &mut catsum);
        cj = ce;
        i = j;
    }
    cols
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let t0 = Instant::now();
    let prof = std::env::var("SS_PROFILE").is_ok();
    let lap = |name: &str| {
        if prof {
            eprintln!("[{:>7.3}s] {}", t0.elapsed().as_secs_f64(), name);
        }
    };
    if let Some(n) = std::env::var("NCPU").ok().and_then(|v| v.parse::<usize>().ok()) {
        if n > 0 {
            rayon::ThreadPoolBuilder::new()
                .num_threads(n)
                .build_global()
                .ok();
        }
    }

    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());
    let out_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".to_string());
    let data_path = Path::new(&data_dir);
    let out_path = Path::new(&out_dir);

    let manifest_bytes = fs::read(data_path.join("manifest.json")).expect("manifest.json");
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).expect("parse manifest");
    let win_start = parse_ts_ms(manifest.window_start.as_bytes()).expect("win_start");
    let win_end = parse_ts_ms(manifest.window_end.as_bytes()).expect("win_end");
    let click_win_start = win_start - SEVEN_DAYS_MS;

    // ---- dimensions -------------------------------------------------------
    let mut cat_to_idx: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
    let mut cat_names: Vec<String> = vec!["unknown".to_string()];
    cat_to_idx.insert(b"unknown".as_slice().into(), 0);
    let mut prod_map: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
    if let Ok(bytes) = fs::read(data_path.join("products.csv")) {
        if let Some(h) = memchr(b'\n', &bytes) {
            for_each_line(&bytes[h + 1..], |line| {
                if let Some([pid, cat]) = split_strict_line::<2>(line) {
                    if !pid.is_empty() && !cat.is_empty() {
                        let cidx = match cat_to_idx.get(cat) {
                            Some(&i) => i,
                            None => {
                                let i = cat_names.len() as u32;
                                cat_names.push(String::from_utf8_lossy(cat).into_owned());
                                cat_to_idx.insert(cat.into(), i);
                                i
                            }
                        };
                        prod_map.insert(pid.into(), cidx);
                    }
                }
            });
        }
    }
    // rank of each category name in byte-wise order (for tie-breaks)
    let mut order: Vec<usize> = (0..cat_names.len()).collect();
    order.sort_by(|&a, &b| cat_names[a].as_bytes().cmp(cat_names[b].as_bytes()));
    let mut cat_rank = vec![0u32; cat_names.len() + 1];
    for (rank, &i) in order.iter().enumerate() {
        cat_rank[i] = rank as u32;
    }
    // slot used as "no category yet" sentinel (u32::MAX index is clamped below)
    let mut camp_map: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
    let mut camp_names: Vec<String> = vec!["organic".to_string()];
    let mut camp_channels: Vec<String> = vec!["organic".to_string()];
    camp_map.insert(b"organic".as_slice().into(), 0);
    if let Ok(bytes) = fs::read(data_path.join("campaigns.csv")) {
        if let Some(h) = memchr(b'\n', &bytes) {
            for_each_line(&bytes[h + 1..], |line| {
                if let Some([cid, chan]) = split_strict_line::<2>(line) {
                    if !cid.is_empty() && !chan.is_empty() {
                        let idx = camp_names.len() as u32;
                        camp_names.push(String::from_utf8_lossy(cid).into_owned());
                        camp_channels.push(String::from_utf8_lossy(chan).into_owned());
                        camp_map.insert(cid.into(), idx);
                    }
                }
            });
        }
    }
    lap("dims");

    // ---- links -> person map ---------------------------------------------
    let interner = Interner::new();
    let link_bufs: Vec<Vec<u8>> = list_csv_files(&data_path.join("links"))
        .par_iter()
        .map(|p| fs::read(p).unwrap_or_default())
        .collect();
    let person_of = build_person_map(&interner, &link_bufs);
    drop(link_bufs);
    lap("links");

    // ---- clicks -----------------------------------------------------------
    let click_bufs: Vec<Vec<u8>> = list_csv_files(&data_path.join("clicks"))
        .par_iter()
        .map(|p| fs::read(p).unwrap_or_default())
        .collect();
    let click_buckets: Vec<Mutex<Vec<Vec<ClickRec<'_>>>>> =
        (0..NPB).map(|_| Mutex::new(Vec::new())).collect();
    {
        let mut cranges = Vec::new();
        for (fi, b) in click_bufs.iter().enumerate() {
            for (s, e) in chunk_ranges(b, MEM_CHUNK) {
                cranges.push((fi, s, e));
            }
        }
        cranges.par_iter().for_each(|&(fi, s, e)| {
            let body = &click_bufs[fi][s..e];
            let mut recs: Vec<ClickRec<'_>> = Vec::new();
            let mut uids: Vec<&[u8]> = Vec::new();
            for_each_line(body, |line| {
                let Some([cid, uid, camp_id, ts_s]) = split_strict_line::<4>(line) else {
                    return;
                };
                if cid.is_empty() || uid.is_empty() || camp_id.is_empty() || camp_id == b"organic" {
                    return;
                }
                let Some(&camp) = camp_map.get(camp_id) else {
                    return;
                };
                let ts = match parse_ts_ms(ts_s) {
                    Some(v) if v >= click_win_start && v < win_end => v,
                    _ => return,
                };
                uids.push(uid);
                recs.push(ClickRec {
                    ts,
                    pid: 0,
                    camp,
                    id: cid,
                });
            });
            let mut raws = vec![0u32; uids.len()];
            resolve_ids(&interner, &uids, &mut raws);
            let mut outv: Vec<Vec<ClickRec<'_>>> = (0..NPB).map(|_| Vec::new()).collect();
            for (k, mut r) in recs.into_iter().enumerate() {
                r.pid = person(&person_of, raws[k]);
                outv[(r.pid as usize) & (NPB - 1)].push(r);
            }
            for (pb, v) in outv.into_iter().enumerate() {
                if !v.is_empty() {
                    click_buckets[pb].lock().unwrap().push(v);
                }
            }
        });
    }
    lap("clicks");

    // ---- events: streamed, chunk-parallel parse ---------------------------
    let files: Vec<(File, u64)> = list_csv_files(&data_path.join("events"))
        .iter()
        .filter_map(|p| {
            let f = File::open(p).ok()?;
            let len = f.metadata().ok()?.len();
            Some((f, len))
        })
        .collect();
    let mut work: Vec<(usize, u64, u64)> = Vec::new();
    for (fi, &(_, len)) in files.iter().enumerate() {
        let mut s = 0u64;
        while s < len {
            let e = (s + EVENT_CHUNK).min(len);
            work.push((fi, s, e));
            s = e;
        }
    }
    let ev_buckets: Vec<Mutex<Vec<Vec<Rec>>>> = (0..NB).map(|_| Mutex::new(Vec::new())).collect();
    {
        let ctx = EvCtx {
            win_start,
            win_end,
            prod_map: &prod_map,
            interner: &interner,
            person_of: &person_of,
            buckets: &ev_buckets,
        };
        work.par_iter().for_each(|&(fi, s, e)| {
            let (f, len) = &files[fi];
            let (buf, bs) = read_chunk(f, *len, s, e);
            process_event_chunk(&ctx, &buf[bs..]);
        });
    }
    drop(files);
    lap("events parse");

    // ---- dedup + scatter by person ---------------------------------------
    let person_buckets: Vec<Mutex<Vec<Vec<EvOut>>>> =
        (0..NPB).map(|_| Mutex::new(Vec::new())).collect();
    (0..NB)
        .into_par_iter()
        .for_each(|b| dedup_bucket(b, &ev_buckets, &person_buckets));
    drop(ev_buckets);
    lap("dedup");

    // ---- sessionize + attribute ------------------------------------------
    let ws_day = win_start.div_euclid(DAY_MS);
    let n_days = ((win_end - 1).div_euclid(DAY_MS) - ws_day + 1).max(1) as usize;
    let n_camp = camp_names.len();
    let cx = AggCtx {
        cat_rank: &cat_rank,
        ws_day,
    };
    let (user_cols, camp_acc): (Vec<UserCols>, CampAcc) = (0..NPB)
        .into_par_iter()
        .fold(
            || (Vec::new(), CampAcc::new(n_camp, n_days)),
            |(mut v, mut acc), pb| {
                let cols = process_person_bucket(pb, &person_buckets, &click_buckets, &cx, &mut acc);
                v.push(cols);
                (v, acc)
            },
        )
        .reduce(
            || (Vec::new(), CampAcc::new(n_camp, n_days)),
            |(mut v1, mut a1), (v2, a2)| {
                v1.extend(v2);
                a1.merge(a2);
                (v1, a1)
            },
        );
    lap("sessionize");

    // ---- output -----------------------------------------------------------
    let shards: Vec<Shard> = interner
        .shards
        .into_iter()
        .map(|m| m.into_inner().unwrap())
        .collect();
    fs::create_dir_all(out_path.join("user_daily")).expect("mkdir user_daily");
    fs::create_dir_all(out_path.join("campaign_daily")).expect("mkdir campaign_daily");
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    let u_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    (0..NPARTS).into_par_iter().for_each(|part| {
        let file = File::create(
            out_path
                .join("user_daily")
                .join(format!("part-{}.parquet", part)),
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(file, u_schema.clone(), Some(props.clone())).unwrap();
        for (k, c) in user_cols.iter().enumerate() {
            if k % NPARTS != part || c.pid.is_empty() {
                continue;
            }
            let n = c.pid.len();
            let mut ub = StringBuilder::with_capacity(n, n * 12);
            for &p in &c.pid {
                let nm = &shards[(p as usize) & (NSHARD - 1)].names[(p >> 8) as usize];
                ub.append_value(String::from_utf8_lossy(nm));
            }
            let mut tb = StringBuilder::with_capacity(n, n * 8);
            for &ci in &c.cat {
                if ci == u32::MAX {
                    tb.append_null();
                } else {
                    tb.append_value(&cat_names[ci as usize]);
                }
            }
            let cols: Vec<ArrayRef> = vec![
                Arc::new(ub.finish()),
                Arc::new(Date32Array::from(c.day.clone())),
                Arc::new(Int64Array::from(c.sessions.clone())),
                Arc::new(Int64Array::from(c.events.clone())),
                Arc::new(Int64Array::from(c.rev.clone())),
                Arc::new(tb.finish()),
            ];
            w.write(&RecordBatch::try_new(u_schema.clone(), cols).unwrap())
                .unwrap();
        }
        w.close().unwrap();
    });

    let c_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let (mut c_ids, mut c_chans, mut c_days) = (Vec::new(), Vec::new(), Vec::new());
    let (mut c_sess, mut c_psess, mut c_rev, mut c_asess) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut emit = |camp: u32, day_idx: i64, v: [i64; 4]| {
        if v[0] > 0 || v[3] > 0 {
            c_ids.push(camp_names[camp as usize].as_str());
            c_chans.push(camp_channels[camp as usize].as_str());
            c_days.push((ws_day + day_idx) as i32);
            c_sess.push(v[0]);
            c_psess.push(v[1]);
            c_rev.push(v[2]);
            c_asess.push(v[3]);
        }
    };
    if camp_acc.dense {
        for camp in 0..n_camp {
            for d in 0..camp_acc.n_days {
                emit(camp as u32, d as i64, camp_acc.cells[camp * camp_acc.n_days + d]);
            }
        }
    } else {
        for (&(camp, d), &v) in &camp_acc.sparse {
            emit(camp, d, v);
        }
    }
    let c_batch = RecordBatch::try_new(
        c_schema.clone(),
        vec![
            Arc::new(arrow::array::StringArray::from(c_ids)) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(c_chans)),
            Arc::new(Date32Array::from(c_days)),
            Arc::new(Int64Array::from(c_sess)),
            Arc::new(Int64Array::from(c_psess)),
            Arc::new(Int64Array::from(c_rev)),
            Arc::new(Int64Array::from(c_asess)),
        ],
    )
    .unwrap();
    let c_file = File::create(out_path.join("campaign_daily").join("part-0.parquet")).unwrap();
    let mut c_writer = ArrowWriter::try_new(c_file, c_schema, Some(props)).unwrap();
    c_writer.write(&c_batch).unwrap();
    c_writer.close().unwrap();
    lap("write");
}
