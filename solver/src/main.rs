use arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use memchr::{memchr, memchr3_iter};
use memmap2::Mmap;
use mimalloc::MiMalloc;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use xxhash_rust::xxh3::xxh3_64;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const DAY_MS: i64 = 86_400_000;
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const TWENTY_FOUR_HOURS_MS: i64 = DAY_MS;
const SESSION_GAP_MS: i64 = 1_800_000;

/// Event-dedup buckets (by event-id hash) and person buckets.
const NB: usize = 256;
const NPB: usize = 512;
const CHUNK: usize = 4 << 20;
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

fn parse_ts_ms_slow(s: &[u8]) -> Option<i64> {
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


/// 8 ASCII digits (first char in the lowest byte) -> value, or None if any is not a digit.
#[inline(always)]
fn swar8(v: u64) -> Option<u64> {
    let t = v.wrapping_add(0x0606_0606_0606_0606);
    if ((v & 0xF0F0_F0F0_F0F0_F0F0) | ((t & 0xF0F0_F0F0_F0F0_F0F0) >> 4)) != 0x3333_3333_3333_3333 {
        return None;
    }
    let mut x = v & 0x0F0F_0F0F_0F0F_0F0F;
    x = x.wrapping_mul(2561) >> 8;
    x = (x & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601) >> 16;
    x = (x & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001) >> 32;
    Some(x)
}

#[inline(always)]
fn dg(b: u8) -> u32 {
    b.wrapping_sub(b'0') as u32
}

/// `YYYY-MM-DDTHH:MM:SS` -> seconds since the epoch (None when malformed or impossible).
#[inline(always)]
fn parse_f1_prefix(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let (d0, d1, d2, d3) = (dg(s[0]), dg(s[1]), dg(s[2]), dg(s[3]));
    let (m0, m1, e0, e1) = (dg(s[5]), dg(s[6]), dg(s[8]), dg(s[9]));
    let (h0, h1, i0, i1, c0, c1) = (dg(s[11]), dg(s[12]), dg(s[14]), dg(s[15]), dg(s[17]), dg(s[18]));
    if (d0 > 9) | (d1 > 9) | (d2 > 9) | (d3 > 9) | (m0 > 9) | (m1 > 9) | (e0 > 9) | (e1 > 9)
        | (h0 > 9) | (h1 > 9) | (i0 > 9) | (i1 > 9) | (c0 > 9) | (c1 > 9)
    {
        return None;
    }
    let yr = (d0 * 1000 + d1 * 100 + d2 * 10 + d3) as i64;
    let mo = (m0 * 10 + m1) as i64;
    let dy = (e0 * 10 + e1) as i64;
    let hr = (h0 * 10 + h1) as i64;
    let mi = (i0 * 10 + i1) as i64;
    let sc = (c0 * 10 + c1) as i64;
    if !(1..=12).contains(&mo) || hr > 23 || mi > 59 || sc > 59 {
        return None;
    }
    if dy < 1 || dy > days_in_month(yr, mo) {
        return None;
    }
    Some(days_from_civil(yr, mo, dy) * 86_400 + hr * 3600 + mi * 60 + sc)
}

#[inline]
fn parse_ts_ms(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if dg(s[0]) <= 9 {
        if n == 20 && s[19] == b'Z' {
            return parse_f1_prefix(s).map(|t| t * 1000);
        }
        if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
            let secs = parse_f1_prefix(s)?;
            let (a, b, c, d) = (dg(s[20]), dg(s[21]), dg(s[23]), dg(s[24]));
            if (a > 9) | (b > 9) | (c > 9) | (d > 9) {
                return None;
            }
            let (oh, om) = ((a * 10 + b) as i64, (c * 10 + d) as i64);
            if oh > 23 || om > 59 {
                return None;
            }
            let off = oh * 3600 + om * 60;
            let t = if s[19] == b'+' { secs - off } else { secs + off };
            return Some(t * 1000);
        }
        if (8..=19).contains(&n) {
            let mut v = swar8(u64::from_le_bytes(s[..8].try_into().unwrap()))?;
            for &b in &s[8..] {
                let d = dg(b);
                if d > 9 {
                    return None;
                }
                v = v * 10 + d as u64;
            }
            return if v > i64::MAX as u64 { None } else { Some(v as i64) };
        }
    }
    parse_ts_ms_slow(s)
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
// Line helpers
// ---------------------------------------------------------------------------

#[inline(always)]
fn for_each_line<'a, F: FnMut(&'a [u8])>(body: &'a [u8], mut f: F) {
    let mut start = 0;
    for nl in memchr::memchr_iter(b'\n', body) {
        f(&body[start..nl]);
        start = nl + 1;
    }
    if start < body.len() {
        f(&body[start..]);
    }
}

#[inline(always)]
fn field_of<'a>(
    body: &'a [u8],
    fstart: usize,
    p: usize,
    qn: u32,
    qfirst: usize,
    qlast: usize,
) -> Option<&'a [u8]> {
    if qn == 0 {
        Some(&body[fstart..p])
    } else if qn == 2 && qfirst == fstart && qlast + 1 == p {
        Some(&body[fstart + 1..p - 1])
    } else {
        None
    }
}

/// One SIMD pass over `body` (whole lines, header already removed). Calls `f` with the N
/// unquoted fields of every well-formed line; malformed lines (wrong field count, a quote
/// anywhere but around a whole field, empty line) are skipped.
#[inline(always)]
fn scan_lines<'a, const N: usize, F: FnMut([&'a [u8]; N])>(body: &'a [u8], mut f: F) {
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut idx = 0usize;
    let mut fstart = 0usize;
    let mut qn = 0u32;
    let mut qfirst = 0usize;
    let mut qlast = 0usize;
    let mut bad = false;
    for p in memchr3_iter(b',', b'"', b'\n', body) {
        let c = body[p];
        if c == b'"' {
            if qn == 0 {
                qfirst = p;
            }
            qlast = p;
            qn += 1;
            continue;
        }
        if !bad {
            if idx >= N {
                bad = true;
            } else {
                match field_of(body, fstart, p, qn, qfirst, qlast) {
                    Some(s) => fields[idx] = s,
                    None => bad = true,
                }
            }
        }
        if c == b'\n' {
            if !bad && idx == N - 1 {
                f(fields);
            }
            idx = 0;
            bad = false;
        } else {
            idx += 1;
        }
        fstart = p + 1;
        qn = 0;
    }
    if !body.is_empty() && body[body.len() - 1] != b'\n' && !bad && idx == N - 1 {
        if let Some(s) = field_of(body, fstart, body.len(), qn, qfirst, qlast) {
            fields[idx] = s;
            f(fields);
        }
    }
}

/// Same contract as `scan_lines`, with 64-byte AVX2 delimiter bitmasks.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,popcnt")]
unsafe fn scan_avx2<'a, const N: usize, F: FnMut([&'a [u8]; N])>(body: &'a [u8], mut f: F) {
    use std::arch::x86_64::*;
    let len = body.len();
    let cc = _mm256_set1_epi8(b',' as i8);
    let qc = _mm256_set1_epi8(b'"' as i8);
    let nc = _mm256_set1_epi8(b'\n' as i8);
    macro_rules! mask {
        ($lo:expr, $hi:expr, $c:expr) => {
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8($lo, $c)) as u32 as u64)
                | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8($hi, $c)) as u32 as u64) << 32)
        };
    }
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut idx = 0usize;
    let mut fstart = 0usize;
    let mut qbase = 0u32; // quotes before the current 64-byte block
    let mut line_q0 = 0u32; // quotes before the current line
    let mut qfields = 0u32;
    let mut bad = false;
    macro_rules! finish {
        ($abs:expr) => {
            if !bad {
                if idx >= N {
                    bad = true;
                } else {
                    let fs = fstart;
                    let flen = $abs - fs;
                    let first_q = flen > 0 && *body.get_unchecked(fs) == b'"';
                    let last_q = flen > 0 && *body.get_unchecked($abs - 1) == b'"';
                    if first_q != last_q || (first_q && flen < 2) {
                        bad = true;
                    } else if first_q {
                        *fields.get_unchecked_mut(idx) = body.get_unchecked(fs + 1..$abs - 1);
                        qfields += 1;
                    } else {
                        *fields.get_unchecked_mut(idx) = body.get_unchecked(fs..$abs);
                    }
                }
            }
        };
    }
    let mut tmp = [0u8; 64];
    let mut pos = 0usize;
    while pos < len {
        let p: *const u8 = if pos + 64 <= len {
            body.as_ptr().add(pos)
        } else {
            tmp = [0u8; 64];
            tmp[..len - pos].copy_from_slice(&body[pos..]);
            tmp.as_ptr()
        };
        let lo = _mm256_loadu_si256(p as *const __m256i);
        let hi = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let cm = mask!(lo, hi, cc);
        let qm = mask!(lo, hi, qc);
        let nm = mask!(lo, hi, nc);
        let mut dm = cm | nm;
        while dm != 0 {
            let b = dm.trailing_zeros();
            dm &= dm - 1;
            let abs = pos + b as usize;
            finish!(abs);
            if (nm >> b) & 1 != 0 {
                let upto = qbase + (qm & ((1u64 << b) - 1)).count_ones();
                if !bad && idx == N - 1 && upto - line_q0 == 2 * qfields {
                    f(fields);
                }
                line_q0 = upto;
                idx = 0;
                bad = false;
                qfields = 0;
            } else {
                idx += 1;
            }
            fstart = abs + 1;
        }
        qbase += qm.count_ones();
        pos += 64;
    }
    if len > 0 && *body.get_unchecked(len - 1) != b'\n' {
        finish!(len);
        if !bad && idx == N - 1 && qbase - line_q0 == 2 * qfields {
            f(fields);
        }
    }
}

#[inline(always)]
fn scan<'a, const N: usize, F: FnMut([&'a [u8]; N])>(body: &'a [u8], f: F) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { scan_avx2::<N, F>(body, f) };
        }
    }
    scan_lines::<N, F>(body, f)
}

/// Line-aligned body ranges of an in-memory CSV file (header skipped).
fn chunk_bounds(m: &[u8], target: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = match memchr(b'\n', m) {
        Some(i) => i + 1,
        None => return out,
    };
    while start < m.len() {
        let mut end = (start + target).min(m.len());
        if end < m.len() {
            let probe = end - 1;
            end = match memchr(b'\n', &m[probe..]) {
                Some(i) => probe + i + 1,
                None => m.len(),
            };
        }
        out.push((start, end));
        start = end;
    }
    out
}

// ---------------------------------------------------------------------------
// Memory-mapped inputs and byte references
// ---------------------------------------------------------------------------

fn map_files(paths: &[PathBuf]) -> Vec<Mmap> {
    let mut out = Vec::new();
    for p in paths {
        let Ok(f) = File::open(p) else { continue };
        let Ok(md) = f.metadata() else { continue };
        if md.len() == 0 {
            continue;
        }
        assert!(md.len() < (1u64 << 32), "input file too large");
        if let Ok(m) = unsafe { Mmap::map(&f) } {
            #[cfg(unix)]
            let _ = m.advise(memmap2::Advice::WillNeed);
            out.push(m);
        }
    }
    out
}

/// A reference to a byte string inside the mapped inputs: file id, length and offset.
#[inline(always)]
fn make_ref(fid: usize, off: usize, len: usize) -> u64 {
    ((fid as u64) << 48) | (((len as u64) & 0xFFFF) << 32) | off as u64
}

#[inline(always)]
fn ref_slice<'a>(all: &'a [Mmap], r: u64) -> &'a [u8] {
    let fid = (r >> 48) as usize;
    let len = ((r >> 32) & 0xFFFF) as usize;
    let off = (r & 0xFFFF_FFFF) as usize;
    &all[fid][off..off + len]
}

// ---------------------------------------------------------------------------
// Lock-free ID table: 63-bit id hash -> slot; slot -> reference to the id's bytes
// ---------------------------------------------------------------------------

struct IdTable {
    keys: &'static [AtomicU64],
    refs: &'static [AtomicU64],
    mask: usize,
    count: AtomicUsize,
    limit: usize,
    overflow: AtomicBool,
}

/// Zeroed anonymous memory, asking for transparent huge pages. Intentionally leaked: it lives
/// for the whole run (the rare retry path leaks the previous table).
fn zeroed_atomics(n: usize) -> &'static [AtomicU64] {
    let mut m = memmap2::MmapMut::map_anon(n * 8).expect("table alloc");
    #[cfg(target_os = "linux")]
    let _ = m.advise(memmap2::Advice::HugePage);
    let ptr = m.as_mut_ptr() as *mut AtomicU64;
    std::mem::forget(m);
    unsafe { std::slice::from_raw_parts(ptr, n) }
}

impl IdTable {
    fn new(cap: usize) -> Self {
        assert!(cap.is_power_of_two());
        IdTable {
            keys: zeroed_atomics(cap),
            refs: zeroed_atomics(cap),
            mask: cap - 1,
            count: AtomicUsize::new(0),
            limit: cap / 10 * 7,
            overflow: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    fn prefetch(&self, key: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let s = (key >> 1) as usize & self.mask;
            std::arch::x86_64::_mm_prefetch(
                self.keys.as_ptr().add(s) as *const i8,
                std::arch::x86_64::_MM_HINT_T0,
            );
        }
    }

    /// Returns the slot of `key` (inserting it with `rf` if new); u32::MAX when full.
    #[inline]
    fn get_or_insert(&self, key: u64, rf: u64) -> u32 {
        let mut s = (key >> 1) as usize & self.mask;
        let mut probes = 0usize;
        loop {
            let k = self.keys[s].load(Relaxed);
            if k == key {
                return s as u32;
            }
            if k == 0 {
                match self.keys[s].compare_exchange(0, key, Relaxed, Relaxed) {
                    Ok(_) => {
                        self.refs[s].store(rf, Relaxed);
                        if self.count.fetch_add(1, Relaxed) + 1 > self.limit {
                            self.overflow.store(true, Relaxed);
                        }
                        return s as u32;
                    }
                    Err(cur) => {
                        if cur == key {
                            return s as u32;
                        }
                    }
                }
            }
            s = (s + 1) & self.mask;
            probes += 1;
            if probes > self.mask {
                self.overflow.store(true, Relaxed);
                return u32::MAX;
            }
        }
    }
}

#[inline(always)]
fn id_key(id: &[u8]) -> u64 {
    xxh3_64(id) | 1
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let g = p[p[x as usize] as usize];
        p[x as usize] = g;
        x = g;
    }
    x
}

/// Joins linked ids; the root of each person is the slot of its byte-wise smallest id.
fn build_parent(all: &[Mmap], table: &IdTable, pairs: &[Vec<(u32, u32)>], parent: &mut [u32]) {
    let name = |slot: u32| ref_slice(all, table.refs[slot as usize].load(Relaxed));
    for v in pairs {
        for &(a, b) in v {
            if a == u32::MAX || b == u32::MAX {
                continue;
            }
            let ra = find(parent, a);
            let rb = find(parent, b);
            if ra != rb {
                if name(ra) < name(rb) {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    for v in pairs {
        for &(a, b) in v {
            if a == u32::MAX || b == u32::MAX {
                continue;
            }
            let ra = find(parent, a);
            parent[a as usize] = ra;
            let rb = find(parent, b);
            parent[b as usize] = rb;
        }
    }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// A well-formed event line; `ts == i64::MIN` marks an invalid one (still part of dedup).
#[derive(Clone, Copy)]
struct Rec {
    h: u64,
    ts: i64,
    slot: u32,
    ing: u32,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy)]
struct EvOut {
    ts: i64,
    person: u32,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy)]
struct Click {
    ts: i64,
    who: u32,
    camp: u32,
    id: u64,
}

#[inline(always)]
fn person_bucket(person: u32) -> usize {
    ((person.wrapping_mul(0x9E37_79B1) >> 8) as usize) & (NPB - 1)
}

struct EvCtx<'a> {
    win_start: i64,
    win_end: i64,
    prod_map: &'a FxHashMap<Box<[u8]>, u32>,
    table: &'a IdTable,
    buckets: &'a [Mutex<Vec<Vec<Rec>>>],
}

const PF: usize = 16;

struct Pend {
    rec: Rec,
    key: u64,
    rf: u64,
}

#[inline(always)]
fn flush_ring(table: &IdTable, ring: &mut Vec<Pend>, out: &mut [Vec<Rec>]) {
    for p in ring.drain(..) {
        let slot = table.get_or_insert(p.key, p.rf);
        let mut rec = p.rec;
        if slot == u32::MAX {
            rec.ts = i64::MIN;
        } else {
            rec.slot = slot;
        }
        out[(rec.h >> 56) as usize].push(rec);
    }
}

fn parse_event_chunk(ctx: &EvCtx, fid: usize, mbase: usize, body: &[u8]) {
    let est = body.len() / 70 / NB + 16;
    let mut out: Vec<Vec<Rec>> = (0..NB).map(|_| Vec::with_capacity(est)).collect();
    let mut ring: Vec<Pend> = Vec::with_capacity(PF);
    scan::<7, _>(body, |[eid, uid, ts_s, ing_s, etype, pid, price_s]| {
        if eid.is_empty() {
            return;
        }
        let Some(ing_ms) = parse_ts_ms(ing_s) else {
            return;
        };
        let h = xxh3_64(eid);
        let mut rec = Rec {
            h,
            ts: i64::MIN,
            slot: 0,
            ing: ing_ms.div_euclid(1000).clamp(0, u32::MAX as i64) as u32,
            price: 0,
            cat: 0,
        };
        'v: {
            let et: u8 = match etype {
                b"view" => 1,
                b"cart" => 2,
                b"purchase" => 3,
                _ => break 'v,
            };
            if uid.is_empty() || pid.is_empty() {
                break 'v;
            }
            let ts = match parse_ts_ms(ts_s) {
                Some(v) if v >= ctx.win_start && v < ctx.win_end => v,
                _ => break 'v,
            };
            if ing_ms - ts > TWENTY_FOUR_HOURS_MS {
                break 'v;
            }
            if et == 3 {
                match parse_ascii_digits_u64(price_s) {
                    Some(v) if (1..=10_000_000).contains(&v) => rec.price = v as u32,
                    _ => break 'v,
                }
                rec.cat = *ctx.prod_map.get(pid).unwrap_or(&0);
            }
            rec.ts = ts;
            let key = id_key(uid);
            ctx.table.prefetch(key);
            if ring.len() == PF {
                flush_ring(ctx.table, &mut ring, &mut out);
            }
            ring.push(Pend {
                rec,
                key,
                rf: make_ref(fid, uid.as_ptr() as usize - mbase, uid.len()),
            });
            return;
        }
        out[(h >> 56) as usize].push(rec);
    });
    flush_ring(ctx.table, &mut ring, &mut out);
    for (b, v) in out.into_iter().enumerate() {
        if !v.is_empty() {
            ctx.buckets[b].lock().unwrap().push(v);
        }
    }
}

/// Dedups one hash bucket (smallest ingest wins), drops invalid kept copies and scatters
/// the survivors into person buckets.
fn dedup_bucket(
    b: usize,
    ev_buckets: &[Mutex<Vec<Vec<Rec>>>],
    parent: &[u32],
    pbuckets: &[Mutex<Vec<Vec<EvOut>>>],
) {
    let parts = std::mem::take(&mut *ev_buckets[b].lock().unwrap());
    let total: usize = parts.iter().map(|p| p.len()).sum();
    if total == 0 {
        return;
    }
    let mut recs: Vec<Rec> = Vec::with_capacity(total);
    for p in &parts {
        recs.extend_from_slice(p);
    }
    drop(parts);
    let cap = (total * 2).next_power_of_two().max(16);
    let mask = cap - 1;
    let mut keys = vec![0u64; cap];
    let mut idxs = vec![u32::MAX; cap];
    for i in 0..recs.len() {
        let (h, ing) = (recs[i].h, recs[i].ing);
        let mut s = (h as usize) & mask;
        loop {
            let j = idxs[s];
            if j == u32::MAX {
                keys[s] = h;
                idxs[s] = i as u32;
                break;
            }
            if keys[s] == h {
                if ing < recs[j as usize].ing {
                    idxs[s] = i as u32;
                }
                break;
            }
            s = (s + 1) & mask;
        }
    }
    let est = total / NPB + 8;
    let mut local: Vec<Vec<EvOut>> = (0..NPB).map(|_| Vec::with_capacity(est)).collect();
    for &j in &idxs {
        if j == u32::MAX {
            continue;
        }
        let r = &recs[j as usize];
        if r.ts != i64::MIN {
            let person = parent[r.slot as usize];
            local[person_bucket(person)].push(EvOut {
                ts: r.ts,
                person,
                price: r.price,
                cat: r.cat,
            });
        }
    }
    for (pb, v) in local.into_iter().enumerate() {
        if !v.is_empty() {
            pbuckets[pb].lock().unwrap().push(v);
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
    clicks: &[Click],
    cx: &AggCtx,
    acc: &mut CampAcc,
    cols: &mut UserCols,
    catsum: &mut Vec<(u32, i64)>,
) {
    let pid = ev[0].person;
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

fn process_person_bucket(
    pb: usize,
    ev_parts: &[Mutex<Vec<Vec<EvOut>>>],
    click_parts: &[Mutex<Vec<Vec<Click>>>],
    all: &[Mmap],
    cx: &AggCtx,
    acc: &mut CampAcc,
) -> UserCols {
    let eparts = std::mem::take(&mut *ev_parts[pb].lock().unwrap());
    let cparts = std::mem::take(&mut *click_parts[pb].lock().unwrap());
    let mut cols = UserCols::default();
    let n: usize = eparts.iter().map(|p| p.len()).sum();
    if n == 0 {
        return cols;
    }

    // Group events by person (counting sort over local person ids).
    let mut lmap: FxHashMap<u32, u32> = FxHashMap::with_capacity_and_hasher(n / 8 + 16, Default::default());
    let mut lid: Vec<u32> = Vec::with_capacity(n);
    let mut counts: Vec<u32> = Vec::new();
    for part in &eparts {
        for e in part {
            let next = counts.len() as u32;
            let id = *lmap.entry(e.person).or_insert(next);
            if id == next {
                counts.push(0);
            }
            counts[id as usize] += 1;
            lid.push(id);
        }
    }
    let np = counts.len();
    let mut off = vec![0u32; np + 1];
    for i in 0..np {
        off[i + 1] = off[i] + counts[i];
    }
    let mut cur: Vec<u32> = off[..np].to_vec();
    let mut sorted: Vec<EvOut> = vec![
        EvOut {
            ts: 0,
            person: 0,
            price: 0,
            cat: 0
        };
        n
    ];
    let mut k = 0;
    for part in &eparts {
        for e in part {
            let id = lid[k] as usize;
            sorted[cur[id] as usize] = *e;
            cur[id] += 1;
            k += 1;
        }
    }
    drop(eparts);
    drop(lid);

    // Group clicks by the same local ids.
    let nc: usize = cparts.iter().map(|p| p.len()).sum();
    let mut coff = vec![0u32; np + 1];
    let mut csorted: Vec<Click> = Vec::new();
    if nc > 0 {
        let mut cl: Vec<u32> = Vec::with_capacity(nc);
        let mut ccounts = vec![0u32; np];
        for part in &cparts {
            for c in part {
                match lmap.get(&c.who) {
                    Some(&id) => {
                        ccounts[id as usize] += 1;
                        cl.push(id);
                    }
                    None => cl.push(u32::MAX),
                }
            }
        }
        for i in 0..np {
            coff[i + 1] = coff[i] + ccounts[i];
        }
        let mut ccur: Vec<u32> = coff[..np].to_vec();
        csorted = vec![
            Click {
                ts: 0,
                who: 0,
                camp: 0,
                id: 0
            };
            coff[np] as usize
        ];
        let mut k = 0;
        for part in &cparts {
            for c in part {
                let id = cl[k];
                if id != u32::MAX {
                    csorted[ccur[id as usize] as usize] = *c;
                    ccur[id as usize] += 1;
                }
                k += 1;
            }
        }
    }
    drop(cparts);

    let mut catsum: Vec<(u32, i64)> = Vec::new();
    for i in 0..np {
        let ev = &mut sorted[off[i] as usize..off[i + 1] as usize];
        if !ev.windows(2).all(|w| w[0].ts <= w[1].ts) {
            ev.sort_unstable_by_key(|e| e.ts);
        }
        let cl: &mut [Click] = if nc > 0 {
            &mut csorted[coff[i] as usize..coff[i + 1] as usize]
        } else {
            &mut []
        };
        if cl.len() > 1 && !cl.windows(2).all(|w| w[0].ts <= w[1].ts) {
            cl.sort_unstable_by(|a, b| {
                a.ts.cmp(&b.ts)
                    .then_with(|| ref_slice(all, b.id).cmp(ref_slice(all, a.id)))
            });
        }
        // Equal timestamps: smallest click_id must come last.
        if cl.len() > 1 {
            let mut s = 0;
            while s < cl.len() {
                let mut e = s + 1;
                while e < cl.len() && cl[e].ts == cl[s].ts {
                    e += 1;
                }
                if e - s > 1 {
                    cl[s..e].sort_unstable_by(|a, b| ref_slice(all, b.id).cmp(ref_slice(all, a.id)));
                }
                s = e;
            }
        }
        process_person(ev, cl, cx, acc, &mut cols, &mut catsum);
    }
    cols
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

enum Item {
    Ev(usize, usize, usize),
    Ck(usize, usize, usize),
}

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
            rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok();
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
    let mut order: Vec<usize> = (0..cat_names.len()).collect();
    order.sort_by(|&a, &b| cat_names[a].as_bytes().cmp(cat_names[b].as_bytes()));
    let mut cat_rank = vec![0u32; cat_names.len() + 1];
    for (rank, &i) in order.iter().enumerate() {
        cat_rank[i] = rank as u32;
    }
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

    // ---- map inputs -------------------------------------------------------
    let ev_paths = list_csv_files(&data_path.join("events"));
    let ck_paths = list_csv_files(&data_path.join("clicks"));
    let lk_paths = list_csv_files(&data_path.join("links"));
    let mut all_paths: Vec<PathBuf> = Vec::new();
    all_paths.extend(ev_paths.iter().cloned());
    all_paths.extend(ck_paths.iter().cloned());
    all_paths.extend(lk_paths.iter().cloned());
    // files that failed to open or are empty are skipped, so recompute the groups
    let mut all: Vec<Mmap> = Vec::new();
    let mut ev_n = 0usize;
    let mut ck_n = 0usize;
    for (group, paths) in [&ev_paths, &ck_paths, &lk_paths].into_iter().enumerate() {
        let m = map_files(paths);
        match group {
            0 => ev_n = m.len(),
            1 => ck_n = m.len(),
            _ => {}
        }
        all.extend(m);
    }
    let ev_range = 0..ev_n;
    let ck_range = ev_n..ev_n + ck_n;
    let lk_range = ev_n + ck_n..all.len();
    let ev_bytes: usize = all[ev_range.clone()].iter().map(|m| m.len()).sum();
    let lk_bytes: usize = all[lk_range.clone()].iter().map(|m| m.len()).sum();
    lap("map");

    let mut items: Vec<Item> = Vec::new();
    for fid in ev_range.clone() {
        for (s, e) in chunk_bounds(&all[fid], CHUNK) {
            items.push(Item::Ev(fid, s, e));
        }
    }
    for fid in ck_range.clone() {
        for (s, e) in chunk_bounds(&all[fid], CHUNK) {
            items.push(Item::Ck(fid, s, e));
        }
    }
    let mut link_items: Vec<(usize, usize, usize)> = Vec::new();
    for fid in lk_range.clone() {
        for (s, e) in chunk_bounds(&all[fid], CHUNK) {
            link_items.push((fid, s, e));
        }
    }

    // ---- parse (retry with a bigger id table if it fills up) --------------
    let mut cap = (ev_bytes / 400 + lk_bytes / 10).max(1 << 16).next_power_of_two();
    let (table, parent, ev_buckets, click_chunks) = loop {
        let table = IdTable::new(cap);
        let mut parent: Vec<u32> = (0..cap as u32).into_par_iter().collect();
        let ev_buckets: Vec<Mutex<Vec<Vec<Rec>>>> = (0..NB).map(|_| Mutex::new(Vec::new())).collect();
        let click_chunks: Mutex<Vec<Vec<Click>>> = Mutex::new(Vec::new());
        let link_pairs: Mutex<Vec<Vec<(u32, u32)>>> = Mutex::new(Vec::new());

        link_items.par_iter().for_each(|&(fid, s, e)| {
            let mbase = all[fid].as_ptr() as usize;
            let body = &all[fid][s..e];
            let mut pairs: Vec<(u32, u32)> = Vec::new();
            scan::<2, _>(body, |[a, b]| {
                if a.is_empty() || b.is_empty() {
                    return;
                }
                let sa = table.get_or_insert(
                    id_key(a),
                    make_ref(fid, a.as_ptr() as usize - mbase, a.len()),
                );
                let sb = table.get_or_insert(
                    id_key(b),
                    make_ref(fid, b.as_ptr() as usize - mbase, b.len()),
                );
                pairs.push((sa, sb));
            });
            if !pairs.is_empty() {
                link_pairs.lock().unwrap().push(pairs);
            }
        });
        lap("links parsed");
        let link_pairs = link_pairs.into_inner().unwrap();

        let ctx = EvCtx {
            win_start,
            win_end,
            prod_map: &prod_map,
            table: &table,
            buckets: &ev_buckets,
        };
        rayon::scope(|s| {
            s.spawn(|_| {
                build_parent(&all, &table, &link_pairs, &mut parent);
                lap("union-find");
            });
            items.par_iter().for_each(|it| match *it {
                Item::Ev(fid, s, e) => {
                    let mbase = all[fid].as_ptr() as usize;
                    parse_event_chunk(&ctx, fid, mbase, &all[fid][s..e]);
                }
                Item::Ck(fid, s, e) => {
                    let mbase = all[fid].as_ptr() as usize;
                    let mut out: Vec<Click> = Vec::with_capacity((e - s) / 50);
                    scan::<4, _>(&all[fid][s..e], |[cid, uid, camp_id, ts_s]| {
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
                        let slot = table.get_or_insert(
                            id_key(uid),
                            make_ref(fid, uid.as_ptr() as usize - mbase, uid.len()),
                        );
                        if slot == u32::MAX {
                            return;
                        }
                        out.push(Click {
                            ts,
                            who: slot,
                            camp,
                            id: make_ref(fid, cid.as_ptr() as usize - mbase, cid.len()),
                        });
                    });
                    if !out.is_empty() {
                        click_chunks.lock().unwrap().push(out);
                    }
                }
            });
        });
        lap("events+clicks parsed");
        if table.overflow.load(Relaxed) {
            cap *= 4;
            eprintln!("id table overflow, retrying with capacity {}", cap);
            continue;
        }
        break (table, parent, ev_buckets, click_chunks.into_inner().unwrap());
    };

    // ---- dedup + scatter by person ---------------------------------------
    let person_buckets: Vec<Mutex<Vec<Vec<EvOut>>>> = (0..NPB).map(|_| Mutex::new(Vec::new())).collect();
    let click_parts: Vec<Mutex<Vec<Vec<Click>>>> = (0..NPB).map(|_| Mutex::new(Vec::new())).collect();
    rayon::join(
        || {
            (0..NB)
                .into_par_iter()
                .for_each(|b| dedup_bucket(b, &ev_buckets, &parent, &person_buckets));
        },
        || {
            click_chunks.par_iter().for_each(|chunk| {
                let est = chunk.len() / NPB + 8;
                let mut local: Vec<Vec<Click>> = (0..NPB).map(|_| Vec::with_capacity(est)).collect();
                for c in chunk {
                    let person = parent[c.who as usize];
                    let mut c2 = *c;
                    c2.who = person;
                    local[person_bucket(person)].push(c2);
                }
                for (pb, v) in local.into_iter().enumerate() {
                    if !v.is_empty() {
                        click_parts[pb].lock().unwrap().push(v);
                    }
                }
            });
        },
    );
    drop(ev_buckets);
    drop(click_chunks);
    lap("dedup+scatter");

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
                let cols = process_person_bucket(pb, &person_buckets, &click_parts, &all, &cx, &mut acc);
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
    fs::create_dir_all(out_path.join("user_daily")).expect("mkdir user_daily");
    fs::create_dir_all(out_path.join("campaign_daily")).expect("mkdir campaign_daily");
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
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
        let file = File::create(out_path.join("user_daily").join(format!("part-{}.parquet", part))).unwrap();
        let mut w = ArrowWriter::try_new(file, u_schema.clone(), Some(props.clone())).unwrap();
        for (k, c) in user_cols.iter().enumerate() {
            if k % NPARTS != part || c.pid.is_empty() {
                continue;
            }
            let n = c.pid.len();
            let mut ub = StringBuilder::with_capacity(n, n * 12);
            for &p in &c.pid {
                let nm = ref_slice(&all, table.refs[p as usize].load(Relaxed));
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
            w.write(&RecordBatch::try_new(u_schema.clone(), cols).unwrap()).unwrap();
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
    let (mut c_sess, mut c_psess, mut c_rev, mut c_asess) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
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
            Arc::new(StringArray::from(c_ids)) as ArrayRef,
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
    let mut c_writer = ArrowWriter::try_new(c_file, c_schema, Some(props)).unwrap();
    c_writer.write(&c_batch).unwrap();
    c_writer.close().unwrap();
    lap("write");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_fast_matches_slow() {
        let mut seed = 777u64;
        let mut check = |t: &[u8]| {
            assert_eq!(parse_ts_ms(t), parse_ts_ms_slow(t), "{:?}", String::from_utf8_lossy(t));
        };
        let fixed: &[&str] = &[
            "2026-10-03T23:58:10Z", "2026-10-03T19:58:10-04:00", "1791071890000", "0001791071890000",
            "2026-13-01T00:00:00Z", "2026-02-30T00:00:00Z", "2028-02-29T23:59:59Z", "2027-02-29T00:00:00Z",
            "2026-10-03T24:00:00Z", "2026-10-03T10:00:60Z", "2026-10-03T10:00:00+24:00", "2026-10-03T10:00:00+05:60",
            "2026-10-03T10:00:00+0200", "2026-10-03T10:00:00-00:00", "2026-10-03t10:00:00Z", "2026-10-03 10:00:00",
            "2026-10-03T10:00:00z", "1791071890000.0", "1.79e12", "-5", "86400000", "", "Z", "123456789012345678901",
            "99999999999999999999", "9223372036854775807", "9223372036854775808", "18446744073709551615",
            "\u{0662}\u{0665}\u{0660}", "２０２６-10-03T10:00:00Z", "2026-10-03T10:00:00+23:59", "12345678",
            "1234567", "123456789", "00000000", "1234567x9012", "12345678901234567890",
        ];
        for t in fixed { check(t.as_bytes()); }
        let alphabet: &[u8] = b"0123456789-:TZ+ .";
        for _ in 0..300000 {
            let n = (lcg(&mut seed) % 27) as usize;
            let t: Vec<u8> = (0..n).map(|_| alphabet[(lcg(&mut seed) % alphabet.len() as u64) as usize]).collect();
            check(&t);
        }
        // structured: mutate valid timestamps
        for _ in 0..300000 {
            let y = 2020 + lcg(&mut seed) % 12;
            let m = lcg(&mut seed) % 14;
            let d = lcg(&mut seed) % 33;
            let mut t = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}", y, m, d, lcg(&mut seed) % 26, lcg(&mut seed) % 62, lcg(&mut seed) % 62).into_bytes();
            match lcg(&mut seed) % 4 {
                0 => t.push(b'Z'),
                1 => t.extend_from_slice(format!("+{:02}:{:02}", lcg(&mut seed) % 26, lcg(&mut seed) % 62).as_bytes()),
                2 => t.extend_from_slice(format!("-{:02}:{:02}", lcg(&mut seed) % 26, lcg(&mut seed) % 62).as_bytes()),
                _ => { let k = lcg(&mut seed) as usize % t.len(); t[k] = b'x'; t.push(b'Z'); }
            }
            check(&t);
        }
        for _ in 0..300000 {
            let digits = 1 + lcg(&mut seed) % 22;
            let t: Vec<u8> = (0..digits).map(|_| b'0' + (lcg(&mut seed) % 10) as u8).collect();
            check(&t);
        }
    }

    fn collect<const N: usize>(body: &[u8], avx: bool) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let f = |fs: [&[u8]; N]| out.push(fs.iter().map(|x| x.to_vec()).collect::<Vec<_>>());
        if avx {
            unsafe { scan_avx2::<N, _>(body, f) }
        } else {
            scan_lines::<N, _>(body, f)
        }
        out
    }

    fn lcg(s: &mut u64) -> u64 {
        *s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *s >> 33
    }

    #[test]
    fn avx_matches_scalar() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }
        let alphabet: &[u8] = b",,,\"\"\n\nab1 \r";
        let mut seed = 12345u64;
        for iter in 0..30000 {
            let n = (lcg(&mut seed) % 300) as usize;
            let mut body: Vec<u8> = (0..n).map(|_| alphabet[(lcg(&mut seed) % alphabet.len() as u64) as usize]).collect();
            // sometimes structured lines
            if iter % 3 == 0 {
                body.clear();
                for _ in 0..(lcg(&mut seed) % 12) {
                    let nf = 3 + (lcg(&mut seed) % 3) as usize;
                    for k in 0..nf {
                        match lcg(&mut seed) % 5 {
                            0 => body.extend_from_slice(b"\"ab\""),
                            1 => body.extend_from_slice(b"\"\""),
                            2 => body.extend_from_slice(b"xyz"),
                            3 => {}
                            _ => body.extend_from_slice(b"a\"b"),
                        }
                        if k + 1 < nf {
                            body.push(b',');
                        }
                    }
                    if lcg(&mut seed) % 8 != 0 {
                        body.push(b'\n');
                    }
                }
            }
            for _ in 0..(lcg(&mut seed) % 3) {
                if lcg(&mut seed) % 2 == 0 && !body.is_empty() {
                    body.pop();
                }
            }
            assert_eq!(collect::<4>(&body, true), collect::<4>(&body, false), "N=4 {:?}", String::from_utf8_lossy(&body));
            assert_eq!(collect::<3>(&body, true), collect::<3>(&body, false), "N=3 {:?}", String::from_utf8_lossy(&body));
            assert_eq!(collect::<2>(&body, true), collect::<2>(&body, false), "N=2 {:?}", String::from_utf8_lossy(&body));
        }
    }

    #[test]
    fn scalar_handles_unterminated_last_line() {
        let got = collect::<3>(b"a,b,c\nd,e,", false);
        assert_eq!(got.len(), 2);
        assert_eq!(got[1], vec![b"d".to_vec(), b"e".to_vec(), b"".to_vec()]);
    }
}
