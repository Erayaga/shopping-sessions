//! Shopping Sessions: the fast pipeline.
//!
//! Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
//! With OUT_FORMAT=csv it writes $OUTPUT_DIR/{user_daily,campaign_daily}.csv
//! instead (the fixtures' layout), for diffing against the reference.
//!
//! Stages (each a parallel pass over 256 hash buckets, so every hash table a
//! stage touches is small enough to stay in cache):
//!   links   parse -> intern IDs per ID bucket -> union-find -> per-bucket
//!           tables ID -> (name hash, name ref)
//!   events  parse + validate each copy -> by event_id bucket -> dedup ->
//!           survivors by user_id bucket -> resolve person -> by person bucket
//!   clicks  parse + validate -> by user_id bucket -> resolve -> by person bucket
//!   people  per person bucket: sort, sessions, credit, assists, aggregates

mod arena;
mod out;
mod parse;

use arena::{gather, Parts};

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use memmap2::Mmap;
use parse::{split, DAY};
use xxhash_rust::xxh3::xxh3_64;

const WEEK: i64 = 7 * DAY;
const GAP: u32 = 1_800_000;
const WEEK32: u32 = WEEK as u32;
const LATE: i64 = DAY;
const CHUNK: usize = 4 << 20;
const BUCKET_BITS: u32 = 8;
const NB: usize = 1 << BUCKET_BITS;
const INVALID: u32 = u32::MAX;

// ------------------------------------------------------------------ hashing
#[inline(always)]
fn hash(b: &[u8]) -> u64 {
    let h = xxh3_64(b);
    if h == 0 {
        1
    } else {
        h
    }
}

/// The first 8 bytes, big-endian, zero padded: orders IDs byte-wise except
/// when equal, where the full bytes decide.
#[inline(always)]
fn prefix(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    let n = b.len().min(8);
    a[..n].copy_from_slice(&b[..n]);
    u64::from_be_bytes(a)
}

#[inline(always)]
fn bucket(h: u64) -> usize {
    (h >> (64 - BUCKET_BITS)) as usize
}

/// Open addressing: u64 key (a hash, never 0) -> V, keys and values interleaved.
pub struct Table<V: Copy + Default> {
    slots: Vec<(u64, V)>,
    mask: usize,
}

impl<V: Copy + Default> Table<V> {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(4) * 2).next_power_of_two();
        Table { slots: vec![(0, V::default()); cap], mask: cap - 1 }
    }
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(&self.slots[(k as usize) & self.mask] as *const _ as *const i8, std::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<V> {
        let mut i = (k as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            if s.0 == k {
                return Some(s.1);
            }
            if s.0 == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// The slot for `k`, inserting it with V::default() if absent; true if new.
    #[inline(always)]
    pub fn slot(&mut self, k: u64) -> (&mut V, bool) {
        let mut i = (k as usize) & self.mask;
        loop {
            let s = self.slots[i].0;
            if s == k {
                return (&mut self.slots[i].1, false);
            }
            if s == 0 {
                self.slots[i].0 = k;
                return (&mut self.slots[i].1, true);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn insert(&mut self, k: u64, v: V) {
        *self.slot(k).0 = v;
    }
}

// ------------------------------------------------------------------ corpus
/// Every mapped input file in one address space, so a field can be named by
/// one u64: (global offset << 16) | length.
pub struct Corpus {
    maps: Vec<Mmap>,
    bases: Vec<u64>,
}

impl Corpus {
    #[inline]
    pub fn bytes(&self, r: u64) -> &[u8] {
        let off = r >> 16;
        let len = (r & 0xffff) as usize;
        let f = self.bases.partition_point(|&b| b <= off) - 1;
        let s = (off - self.bases[f]) as usize;
        &self.maps[f][s..s + len]
    }
    #[inline(always)]
    fn rf(&self, file: u32, field: &[u8]) -> u64 {
        let m = &self.maps[file as usize];
        let off = self.bases[file as usize] + (field.as_ptr() as usize - m.as_ptr() as usize) as u64;
        (off << 16) | (field.len().min(0xffff) as u64)
    }
}

#[derive(Clone, Copy)]
struct Chunk {
    file: u32,
    start: usize,
    end: usize,
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

/// Maps the files and cuts them into line-aligned chunks (headers skipped).
/// Returns the chunks and the total number of bytes.
fn add_files(maps: &mut Vec<Mmap>, bases: &mut Vec<u64>, next: &mut u64, paths: &[PathBuf]) -> (Vec<Chunk>, usize) {
    let mut chunks = Vec::new();
    let mut total = 0;
    for p in paths {
        let f = File::open(p).expect("open input");
        let len = f.metadata().map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            continue;
        }
        let m = unsafe { Mmap::map(&f).expect("mmap") };
        #[cfg(unix)]
        {
            let _ = m.advise(memmap2::Advice::Sequential);
        }
        let file = maps.len() as u32;
        let data: &[u8] = &m;
        total += data.len();
        if let Some(h) = memchr::memchr(b'\n', data) {
            let mut start = h + 1;
            while start < data.len() {
                let mut end = (start + CHUNK).min(data.len());
                if end < data.len() {
                    end = match memchr::memchr(b'\n', &data[end..]) {
                        Some(i) => end + i + 1,
                        None => data.len(),
                    };
                }
                chunks.push(Chunk { file, start, end });
                start = end;
            }
        }
        bases.push(*next);
        *next += len + 1;
        maps.push(m);
    }
    // biggest first is not needed: chunks are all about CHUNK bytes
    (chunks, total)
}

/// Runs `f(state, i)` for i in 0..n on `threads` threads; returns the states.
fn par<S: Send, F>(threads: usize, n: usize, init: impl Fn() -> S + Sync, f: F) -> Vec<S>
where
    F: Fn(&mut S, usize) + Sync,
{
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// A set of u64 keys (never 0) reused across buckets, reset by its used slots.
#[derive(Default)]
struct LocalSet {
    slots: Vec<u64>,
    used: Vec<u32>,
}

impl LocalSet {
    fn reset(&mut self, expect: usize) {
        for &i in &self.used {
            self.slots[i as usize] = 0;
        }
        self.used.clear();
        let want = (expect.max(16) * 2).next_power_of_two();
        if self.slots.len() < want {
            self.slots = vec![0; want];
        }
    }
    /// True if k was not in the set.
    #[inline(always)]
    fn insert(&mut self, k: u64) -> bool {
        if self.used.len() * 2 >= self.slots.len() {
            let keys: Vec<u64> = self.used.iter().map(|&i| self.slots[i as usize]).collect();
            let n = self.slots.len() * 2;
            self.slots = vec![0; n];
            self.used.clear();
            for k in keys {
                self.insert(k);
            }
        }
        let mask = self.slots.len() - 1;
        let mut i = (k as usize) & mask;
        loop {
            let s = self.slots[i];
            if s == k {
                return false;
            }
            if s == 0 {
                self.slots[i] = k;
                self.used.push(i as u32);
                return true;
            }
            i = (i + 1) & mask;
        }
    }
}

fn part_len<T: Copy>(parts: &[Parts<T>], b: usize) -> usize {
    parts.iter().map(|p| p.segs[b].len()).sum()
}

/// NB output vectors per thread, each pre-sized.
fn buckets<T>(cap: usize) -> Vec<Vec<T>> {
    (0..NB).map(|_| Vec::with_capacity(cap)).collect()
}

/// Like `par`, and runs `fin(state)` on each thread before it exits.
fn par_fin<S: Send, F, G>(threads: usize, n: usize, init: impl Fn() -> S + Sync, f: F, fin: G) -> Vec<S>
where
    F: Fn(&mut S, usize) + Sync,
    G: Fn(&mut S) + Sync,
{
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut st, i);
                    }
                    fin(&mut st);
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn bucket_len_v<T>(parts: &[Vec<Vec<T>>], b: usize) -> usize {
    parts.iter().map(|t| t[b].len()).sum()
}

// ------------------------------------------------------------------ records
/// A valid event copy, in its user_id bucket. Its position there (thread,
/// bucket, index) is what duplicate detection refers to.
#[derive(Clone, Copy, Default)]
struct ERec {
    uh: u64,
    uref: u64,
    ts: u32, // ms since t0 = window_start - 7 days
    pc: u32, // price (24 bits) | category << 24; price 0 = not a purchase
}

/// Every event copy, for duplicate detection: loc is the position of its
/// ERec (thread << 28 | bucket << 20 | index), NOLOC for an invalid copy.
#[derive(Clone, Copy, Default)]
struct DRec {
    eh: u64,
    ingest: i32, // seconds since t0, saturated
    loc: u32,
}
const NOLOC: u32 = u32::MAX;

/// A surviving event in its person bucket (the name travels separately).
#[derive(Clone, Copy)]
struct Ev {
    key: u64,
    ts: u32,
    pc: u32,
}

/// A valid click: key as in Ev; cid is the click_id ref.
#[derive(Clone, Copy)]
struct Ck {
    key: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

/// A user_daily row.
#[derive(Clone, Copy)]
pub struct URow {
    pub name: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: u64,
    pub top: u16, // u16::MAX = null
}

fn read_small(path: &Path) -> Vec<Vec<Vec<u8>>> {
    // products.csv / campaigns.csv: two fields per line, header skipped.
    let data = std::fs::read(path).unwrap_or_default();
    let mut rows = Vec::new();
    let mut lines = data.split(|&c| c == b'\n');
    lines.next();
    for line in lines {
        let mut f = [&b""[..]; 2];
        if split(line, &mut f) && !f[0].is_empty() {
            rows.push(vec![f[0].to_vec(), f[1].to_vec()]);
        }
    }
    rows
}

fn manifest_time(text: &str, key: &str) -> i64 {
    let at = text.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &text[at + key.len() + 2..];
    let q1 = rest.find('"').unwrap() + 1;
    let q2 = rest[q1..].find('"').unwrap() + q1;
    parse::timestamp(rest[q1..q2].as_bytes()).expect("manifest timestamp")
}

fn main() {
    let t0_clock = Instant::now();
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let csv_out = std::env::var("OUT_FORMAT").map(|v| v == "csv").unwrap_or(false);
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from))
        .max(1);
    let lap = |what: &str| eprintln!("[{:>8.3}s] {what}", t0_clock.elapsed().as_secs_f64());

    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_time(&manifest, "window_start");
    let we = manifest_time(&manifest, "window_end");
    let t0 = ws - WEEK; // the click window's start: every kept time is >= t0
    assert!(we - t0 < INVALID as i64, "window too long for 32-bit relative times");
    let ws_rel = (ws - t0) as u32;
    let we_rel = (we - t0) as u32;

    // ---- products -> category index (sorted, "unknown" included), campaigns
    let products = read_small(&data_dir.join("products.csv"));
    let mut cats: Vec<Vec<u8>> = products.iter().map(|r| r[1].clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    assert!(cats.len() <= 255, "too many categories");
    let unknown_cat = cats.binary_search(&b"unknown".to_vec()).unwrap() as u32;
    let mut prod_tab: Table<u32> = Table::with_capacity(products.len());
    for r in &products {
        let c = cats.binary_search(&r[1]).unwrap() as u32;
        prod_tab.insert(hash(&r[0]), c);
    }
    // Fast path: when every product_id is canonical "p<n>" (n without leading
    // zeros), a direct array indexed by n gives the same answers as the table;
    // a non-canonical event product can't be in the file, so it's unknown.
    let canon = |b: &[u8]| -> Option<usize> {
        if b.len() < 2 || b.len() > 8 || b[0] != b'p' || !(b'1'..=b'9').contains(&b[1]) {
            return None;
        }
        let mut n = 0usize;
        for &c in &b[1..] {
            if !c.is_ascii_digit() {
                return None;
            }
            n = n * 10 + (c - b'0') as usize;
        }
        Some(n)
    };
    let direct: Option<Vec<u8>> = {
        let mut max = 0;
        let mut ok = !products.is_empty();
        for r in &products {
            match canon(&r[0]) {
                Some(n) => max = max.max(n),
                None => ok = false,
            }
        }
        (ok && max <= 20_000_000).then(|| {
            let mut v = vec![255u8; max + 1];
            for r in &products {
                v[canon(&r[0]).unwrap()] = cats.binary_search(&r[1]).unwrap() as u8;
            }
            v
        })
    };
    let products_lookup = |b: &[u8]| -> u32 {
        match &direct {
            Some(v) => match canon(b) {
                Some(n) if n < v.len() && v[n] != 255 => v[n] as u32,
                _ => unknown_cat,
            },
            None => prod_tab.get(hash(b)).unwrap_or(unknown_cat),
        }
    };
    let campaigns = read_small(&data_dir.join("campaigns.csv"));
    let mut camp_tab: Table<u32> = Table::with_capacity(campaigns.len());
    for (i, r) in campaigns.iter().enumerate() {
        camp_tab.insert(hash(&r[0]), i as u32);
    }
    let organic = campaigns.len() as u32;

    // ---- map inputs
    let mut maps = Vec::new();
    let mut bases = Vec::new();
    let mut next = 0u64;
    let (link_chunks, link_bytes) = add_files(&mut maps, &mut bases, &mut next, &list_csv(&data_dir.join("links")));
    let (event_chunks, _event_bytes) = add_files(&mut maps, &mut bases, &mut next, &list_csv(&data_dir.join("events")));
    let (click_chunks, _click_bytes) = add_files(&mut maps, &mut bases, &mut next, &list_csv(&data_dir.join("clicks")));
    // Leaked: lives for the whole run, so output can borrow its bytes zero-copy.
    let corpus: &'static Corpus = Box::leak(Box::new(Corpus { maps, bases }));
    lap("mapped");

    // Links don't feed parsing (people are resolved after it), so they run
    // on their own thread while events and clicks are parsed.
    let links_job = || {
        // ================================================================ links
        // Parse: each side goes to its ID bucket as (hash, ref, thread-local slot).
        let est = link_bytes / 18 / NB / threads + 16;
        let lp = par(
            threads,
            link_chunks.len(),
            || (buckets::<(u64, u64, u64, u32)>(est * 2), 0u32),
            |(bk, nedges), i| {
                let c = link_chunks[i];
                let data: &[u8] = &corpus.maps[c.file as usize];
                parse::for_rows::<2>(data, c.start, c.end, |f| {
                    if !f[0].is_empty() && !f[1].is_empty() {
                        let e = *nedges;
                        *nedges += 1;
                        for side in 0..2 {
                            let h = hash(f[side]);
                            bk[bucket(h)].push((h, corpus.rf(c.file, f[side]), prefix(f[side]), e * 2 + side as u32));
                        }
                    }
                });
            },
        );
        // Intern per bucket: local index per distinct ID.
        let (lp_parts, lp_edges): (Vec<Vec<Vec<(u64, u64, u64, u32)>>>, Vec<u32>) = lp.into_iter().unzip();
        struct Interned {
            ids: Vec<(u64, u64, u64)>,   // (hash, ref, prefix) per local index
            slots: Vec<(u32, u32, u32)>, // (thread, slot, local index)
        }
        let interned: Vec<Interned> = {
            let per = par(
                threads,
                NB,
                Vec::<(usize, Interned)>::new,
                |acc, b| {
                    let n = bucket_len_v(&lp_parts, b);
                    let mut tab: Table<u32> = Table::with_capacity(n);
                    let mut ids = Vec::new();
                    let mut slots = Vec::with_capacity(n);
                    for (t, part) in lp_parts.iter().enumerate() {
                        for &(h, r, pf, s) in &part[b] {
                            let (v, new) = tab.slot(h);
                            if new {
                                *v = ids.len() as u32;
                                ids.push((h, r, pf));
                            }
                            slots.push((t as u32, s, *v));
                        }
                    }
                    acc.push((b, Interned { ids, slots }));
                },
            );
            let mut all: Vec<(usize, Interned)> = per.into_iter().flatten().collect();
            all.sort_unstable_by_key(|x| x.0);
            all.into_iter().map(|x| x.1).collect()
        };
        drop(lp_parts);
        lap("links: parsed+interned");
        let mut offs = vec![0u32; NB + 1];
        for b in 0..NB {
            offs[b + 1] = offs[b] + interned[b].ids.len() as u32;
        }
        let n_ids = offs[NB] as usize;
        let mut edges: Vec<Vec<u32>> = lp_edges.iter().map(|&n| vec![0u32; n as usize * 2]).collect();
        for (b, it) in interned.iter().enumerate() {
            for &(t, s, l) in &it.slots {
                edges[t as usize][s as usize] = offs[b] + l;
            }
        }
        lap("links: edges");
        let mut parent: Vec<u32> = (0..n_ids as u32).collect();
        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            let mut r = x;
            while parent[r as usize] != r {
                r = parent[r as usize];
            }
            while parent[x as usize] != r {
                let n = parent[x as usize];
                parent[x as usize] = r;
                x = n;
            }
            r
        }
        for e in &edges {
            for pair in e.chunks_exact(2) {
                let (x, y) = (find(&mut parent, pair[0]), find(&mut parent, pair[1]));
                if x != y {
                    // union by smaller index keeps trees shallow enough with compression
                    if x < y {
                        parent[y as usize] = x;
                    } else {
                        parent[x as usize] = y;
                    }
                }
            }
        }
        drop(edges);
        lap("links: union-find");
        // Flat per-ID arrays in global index order.
        let mut gid: Vec<(u64, u64, u64)> = Vec::with_capacity(n_ids);
        for it in &interned {
            gid.extend_from_slice(&it.ids);
        }
        // best[root] = member with the byte-wise smallest ID
        let mut best: Vec<u32> = (0..n_ids as u32).collect();
        for g in 0..n_ids as u32 {
            let r = find(&mut parent, g);
            if r != g {
                let cur = best[r as usize];
                let (a, c) = (&gid[g as usize], &gid[cur as usize]);
                if a.2 < c.2 || (a.2 == c.2 && corpus.bytes(a.1) < corpus.bytes(c.1)) {
                    best[r as usize] = g;
                }
            }
        }
        lap("links: names");
        // Per ID bucket: ID hash -> (name hash, name ref).
        let people: Vec<Table<(u64, u64)>> = {
            let per = par(threads, NB, Vec::<(usize, Table<(u64, u64)>)>::new, |acc, b| {
                let it = &interned[b];
                let mut tab = Table::with_capacity(it.ids.len());
                for (l, &(h, _, _)) in it.ids.iter().enumerate() {
                    let g = offs[b] + l as u32;
                    let n = &gid[best[parent[g as usize] as usize] as usize];
                    tab.insert(h, (n.0, n.1));
                }
                acc.push((b, tab));
            });
            let mut all: Vec<_> = per.into_iter().flatten().collect();
            all.sort_unstable_by_key(|x| x.0);
            all.into_iter().map(|x| x.1).collect()
        };
        drop((interned, best, parent, gid));
        lap(&format!("links: {n_ids} ids"));
        people
    };
    assert!(threads <= 16, "locations pack the thread into 4 bits");
    let (people, ep_valid, ep_ids, cp) = std::thread::scope(|scope| {
        let links = scope.spawn(links_job);
        let tids = AtomicUsize::new(0);
        // ================================================================ events: parse
        // Valid copies go to their user_id bucket in full; every copy also sends
        // (event_id hash, ingest) to its event_id bucket for duplicate detection.
        // Events and clicks in one pass (no barrier between them): indices
        // past the event chunks are click chunks.
        let ne_chunks = event_chunks.len();
        let ep = par_fin(
            threads,
            ne_chunks + click_chunks.len(),
            || (Parts::<ERec>::new(NB), Parts::<DRec>::new(NB), tids.fetch_add(1, Ordering::Relaxed) as u32, Parts::<Ck>::new(NB)),
            |(uk, dk, tid, bk), i| {
                if i >= ne_chunks {
                    let c = click_chunks[i - ne_chunks];
                    let data: &[u8] = &corpus.maps[c.file as usize];
                    parse::for_rows::<4>(data, c.start, c.end, |f| {
                        if f[0].is_empty() || f[1].is_empty() || f[2].is_empty() {
                            return;
                        }
                        let Some(camp) = camp_tab.get(hash(f[2])) else { return };
                        let Some(ts) = parse::timestamp(f[3]) else { return };
                        if ts < t0 || ts >= we {
                            return;
                        }
                        let uh = hash(f[1]);
                        bk.push(bucket(uh), Ck { key: uh, cid: corpus.rf(c.file, f[0]), ts: (ts - t0) as u32, camp });
                    });
                    return;
                }
                let c = event_chunks[i];
                let data: &[u8] = &corpus.maps[c.file as usize];
                parse::for_rows::<7>(data, c.start, c.end, |f| {
                    if f[0].is_empty() {
                        return;
                    }
                    let eh = hash(f[0]);
                    let ingest_abs = parse::timestamp(f[3]).unwrap_or(i64::MAX);
                    let ingest = ((ingest_abs.saturating_sub(t0)) / 1000).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                    let valid = (|| {
                        if f[1].is_empty() || f[5].is_empty() {
                            return None;
                        }
                        let ts = parse::timestamp(f[2])?;
                        if ts < ws || ts >= we {
                            return None;
                        }
                        let price = match f[4] {
                            b"view" | b"cart" => 0,
                            b"purchase" => parse::price(f[6])?,
                            _ => return None,
                        };
                        if ingest_abs.saturating_sub(ts) > LATE {
                            return None;
                        }
                        Some((ts, price))
                    })();
                    let loc = match valid {
                        Some((ts, price)) => {
                            let cat = products_lookup(f[5]);
                            let uh = hash(f[1]);
                            let b = bucket(uh);
                            let idx = uk.segs[b].count();
                            assert!(idx < 1 << 20, "location index overflow");
                            uk.push(b, ERec { uh, uref: corpus.rf(c.file, f[1]), ts: (ts - t0) as u32, pc: price | (cat << 24) });
                            (*tid << 28) | ((b as u32) << 20) | idx as u32
                        }
                        None => NOLOC,
                    };
                    dk.push(bucket(eh), DRec { eh, ingest, loc });
                });
            },
            |(uk, dk, _, bk)| {
                uk.finish();
                dk.finish();
                bk.finish();
            },
        );
        // Locations name the producing thread by its id: order outputs by it.
        let mut ep = ep;
        ep.sort_unstable_by_key(|x| x.2);
        let mut ep_valid = Vec::new();
        let mut ep_ids = Vec::new();
        let mut cp = Vec::new();
        for (u, d, _, c) in ep {
            ep_valid.push(u);
            ep_ids.push(d);
            cp.push(c);
        }
        lap("events and clicks parsed");
        (links.join().unwrap(), ep_valid, ep_ids, cp)
    });

    // ================================================================ duplicate event_ids
    // Per event_id bucket (second-level groups keep tables tiny): for every
    // event_id seen more than once, keep the first valid copy with the
    // smallest ingest (equal-ingest copies are identical) and send the
    // locations of all other valid copies to their user_id bucket.
    let dl = par(
        threads,
        NB,
        || {
            (
                (0..NB).map(|_| Vec::<u32>::new()).collect::<Vec<_>>(),
                (0..256).map(|_| Vec::<DRec>::new()).collect::<Vec<_>>(),
                Table::<(i32, u32)>::with_capacity(4096),
            )
        },
        |(out, groups, tab), b| {
            for r in gather(&ep_ids, b) {
                groups[((r.eh >> 48) & 255) as usize].push(*r);
            }
            for grp in groups.iter_mut() {
                if grp.len() < 2 {
                    grp.clear();
                    continue;
                }
                let need = (grp.len() * 2).next_power_of_two().max(8);
                if tab.slots.len() < need || tab.slots.len() > need * 8 {
                    *tab = Table::with_capacity(grp.len());
                } else {
                    tab.slots.iter_mut().for_each(|s| s.0 = 0);
                }
                for r in grp.iter() {
                    let (v, new) = tab.slot(r.eh);
                    if new {
                        *v = (r.ingest, 1);
                    } else {
                        v.0 = v.0.min(r.ingest);
                        v.1 += 1;
                    }
                }
                for r in grp.iter() {
                    if r.loc == NOLOC {
                        continue;
                    }
                    let (v, _) = tab.slot(r.eh);
                    if v.1 & 0x7fff_ffff < 2 {
                        continue; // not duplicated
                    }
                    if r.ingest == v.0 && v.1 & 0x8000_0000 == 0 {
                        v.1 |= 0x8000_0000; // the kept copy
                    } else {
                        out[((r.loc >> 20) & 0xff) as usize].push(r.loc);
                    }
                }
                grp.clear();
            }
        },
    );
    drop(ep_ids);
    let losers: Vec<Vec<Vec<u32>>> = dl.into_iter().map(|d| d.0).collect();
    let nlosers: usize = losers.iter().flatten().map(|v| v.len()).sum();
    lap(&format!("duplicates: {nlosers} losing copies"));

    // ================================================================ resolve people per user_id bucket
    // Skips the losing copies (by position), resolves each user_id to its
    // person, and sends each person's name once per bucket on a side stream.
    let rp = par_fin(
        threads,
        NB,
        || (Parts::<Ev>::new(NB), Parts::<Ck>::new(NB), Parts::<(u64, u64)>::new(NB), LocalSet::default(), Vec::<Vec<u64>>::new()),
        |(eo, co, no, seen, bits), b| {
            let tab = &people[b];
            // loser bitmaps per producing thread
            bits.resize(ep_valid.len(), Vec::new());
            for (t, part) in ep_valid.iter().enumerate() {
                let w = part.segs[b].len().div_ceil(64);
                bits[t].clear();
                bits[t].resize(w, 0);
            }
            for t in &losers {
                for &loc in &t[b] {
                    let (th, i) = ((loc >> 28) as usize, (loc & 0xf_ffff) as usize);
                    bits[th][i >> 6] |= 1 << (i & 63);
                }
            }
            seen.reset(part_len(&ep_valid, b) / 8);
            for (t, part) in ep_valid.iter().enumerate() {
                let bt = &bits[t];
                for (i, e) in part.segs[b].iter().enumerate() {
                    if bt[i >> 6] & (1 << (i & 63)) != 0 {
                        continue;
                    }
                    let (key, r) = tab.get(e.uh).unwrap_or((e.uh, e.uref));
                    let pb = bucket(key);
                    eo.push(pb, Ev { key, ts: e.ts, pc: e.pc });
                    if seen.insert(key) {
                        no.push(pb, (key, r));
                    }
                }
            }
            for c in gather(&cp, b) {
                let key = tab.get(c.key).map_or(c.key, |x| x.0);
                co.push(bucket(key), Ck { key, ..*c });
            }
        },
        |(eo, co, no, _, _)| {
            eo.finish();
            co.finish();
            no.finish();
        },
    );
    drop(ep_valid);
    drop(cp);
    drop(losers);
    let mut ev_parts = Vec::new();
    let mut ck_parts = Vec::new();
    let mut nm_parts = Vec::new();
    for (e, c, n, _, _) in rp {
        ev_parts.push(e);
        ck_parts.push(c);
        nm_parts.push(n);
    }
    lap("resolved");

    // ================================================================ per person bucket
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let ncamp = campaigns.len() + 1;
    let ncats = cats.len();
    let ctx = PCtx { t0, day0, ndays, organic, ws_rel, we_rel };
    let cats: &'static [Vec<u8>] = Box::leak(cats.into_boxed_slice());
    let octx = out::Ctx { corpus, cats, campaigns: &campaigns, organic };
    let ud_dir = out_dir.join("user_daily");
    if !csv_out {
        std::fs::create_dir_all(&ud_dir).unwrap();
        std::fs::create_dir_all(out_dir.join("campaign_daily")).unwrap();
    }
    let part_no = AtomicUsize::new(0);
    let results = par_fin(
        threads,
        NB,
        || Worker {
            rows: Vec::new(),
            camp: vec![[0u64; 4]; ncamp * ndays],
            cat_sum: vec![0u64; ncats],
            cat_touched: Vec::new(),
            starts: Vec::new(),
            runs: Vec::new(),
            run_of: Vec::new(),
            by_camp: Vec::new(),
            ltab: Vec::new(),
            cnt: Vec::new(),
            idx: Vec::new(),
            es: Vec::new(),
            cs: Vec::new(),
            ccnt: Vec::new(),
            pnames: Vec::new(),
            pkeys: Vec::new(),
        },
        |w, b| {
            w.bucket(&ev_parts, &ck_parts, &nm_parts, b, &ctx, corpus);
        },
        |w| {
            // Overlaps with the threads still working on buckets.
            if !csv_out && !w.rows.is_empty() {
                let n = part_no.fetch_add(1, Ordering::Relaxed);
                out::write_user_part(&ud_dir, n, &octx, &w.rows);
            }
        },
    );
    drop(ev_parts);
    drop(ck_parts);
    lap("sessionized");

    // ---- combine campaign_daily
    let mut camp = vec![[0u64; 4]; ncamp * ndays];
    for w in &results {
        for (a, b) in camp.iter_mut().zip(&w.camp) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    let mut crows: Vec<(u32, i32, [u64; 4])> = Vec::new();
    for c in 0..ncamp {
        for d in 0..ndays {
            let v = camp[c * ndays + d];
            if v[0] > 0 || v[3] > 0 {
                crows.push((c as u32, (day0 + d as i64) as i32, v));
            }
        }
    }
    let user_parts: Vec<Vec<URow>> = results.into_iter().map(|w| w.rows).collect();
    let nrows: usize = user_parts.iter().map(|v| v.len()).sum();

    if csv_out {
        out::write_csv(&out_dir, &octx, &user_parts, &crows);
    } else {
        if part_no.load(Ordering::Relaxed) == 0 {
            // no rows at all: still write one (empty) user_daily file
            out::write_user_part(&ud_dir, 0, &octx, &[]);
        }
        out::write_campaigns(&out_dir.join("campaign_daily"), &octx, &crows);
    }
    lap(&format!("wrote {nrows} user rows, {} campaign rows", crows.len()));
}

#[inline]
fn insertion_sort_by_key<T: Copy, K: Ord>(v: &mut [T], key: impl Fn(&T) -> K) {
    for i in 1..v.len() {
        let x = v[i];
        let kx = key(&x);
        let mut j = i;
        while j > 0 && key(&v[j - 1]) > kx {
            v[j] = v[j - 1];
            j -= 1;
        }
        v[j] = x;
    }
}

struct PCtx {
    t0: i64,
    day0: i64,
    ndays: usize,
    organic: u32,
    #[allow(dead_code)]
    ws_rel: u32,
    #[allow(dead_code)]
    we_rel: u32,
}

struct Worker {
    rows: Vec<URow>,
    camp: Vec<[u64; 4]>,
    cat_sum: Vec<u64>,
    cat_touched: Vec<u8>,
    starts: Vec<u32>,
    runs: Vec<(u32, i32)>, // (first session index, day)
    run_of: Vec<u32>,
    by_camp: Vec<(u32, u32)>,
    ltab: Vec<(u64, u32)>, // person hash -> local index
    cnt: Vec<u32>,         // events per local person, then their start offsets
    idx: Vec<u32>,         // local person of each event / click, in input order
    es: Vec<Ev>,           // events laid out by person
    cs: Vec<Ck>,           // clicks laid out by person
    ccnt: Vec<u32>,        // clicks per local person, then start offsets
    pnames: Vec<u64>,      // name ref per local person
    pkeys: Vec<u64>,       // person hash per local person
}

impl Worker {
    /// One person bucket: a local table numbers the persons, counts give each
    /// one a contiguous range, a single scatter fills it, and only each
    /// person's own events are then sorted by time.
    fn bucket(
        &mut self,
        ev_parts: &[Parts<Ev>],
        ck_parts: &[Parts<Ck>],
        nm_parts: &[Parts<(u64, u64)>],
        b: usize,
        ctx: &PCtx,
        corpus: &Corpus,
    ) {
        let ne: usize = ev_parts.iter().map(|p| p.segs[b].len()).sum();
        if ne == 0 {
            return;
        }
        // Persons are numbered from the name side stream (every person with
        // events here has an entry, possibly repeated), so the table is sized
        // exactly and each local index has its name; events then only probe.
        let nn = part_len(nm_parts, b);
        let want = (nn * 2).next_power_of_two().max(64);
        let mut ltab = std::mem::take(&mut self.ltab);
        if ltab.len() < want || ltab.len() > want * 4 {
            ltab = vec![(0, 0); want];
        }
        let mask = ltab.len() - 1;
        let mut cnt = std::mem::take(&mut self.cnt);
        cnt.clear();
        let mut pnames = std::mem::take(&mut self.pnames);
        pnames.clear();
        let mut pkeys = std::mem::take(&mut self.pkeys);
        pkeys.clear();
        for &(k, r) in gather(nm_parts, b) {
            let mut i = (k as usize) & mask;
            loop {
                let s = ltab[i];
                if s.0 == k {
                    break;
                }
                if s.0 == 0 {
                    ltab[i] = (k, cnt.len() as u32);
                    cnt.push(0);
                    pnames.push(r);
                    pkeys.push(k);
                    break;
                }
                i = (i + 1) & mask;
            }
        }
        let mut idx = std::mem::take(&mut self.idx);
        idx.clear();
        for e in gather(ev_parts, b) {
            let mut i = (e.key as usize) & mask;
            let li = loop {
                let s = ltab[i];
                if s.0 == e.key {
                    break s.1;
                }
                debug_assert!(s.0 != 0, "event of a person without a name");
                i = (i + 1) & mask;
            };
            cnt[li as usize] += 1;
            idx.push(li);
        }
        let np = cnt.len();
        // counts -> start offsets (cnt[np] = total)
        let mut acc = 0u32;
        for c in cnt.iter_mut() {
            let n = *c;
            *c = acc;
            acc += n;
        }
        cnt.push(acc);
        let mut es = std::mem::take(&mut self.es);
        es.clear();
        es.reserve(ne);
        unsafe { es.set_len(ne) };
        {
            let mut pos: Vec<u32> = cnt[..np].to_vec();
            for (j, e) in gather(ev_parts, b).enumerate() {
                let li = idx[j] as usize;
                es[pos[li] as usize] = *e;
                pos[li] += 1;
            }
        }
        // clicks of persons with events (others can credit nothing)
        let mut ccnt = std::mem::take(&mut self.ccnt);
        ccnt.clear();
        ccnt.resize(np + 1, 0);
        idx.clear();
        for c in gather(ck_parts, b) {
            let mut i = (c.key as usize) & mask;
            let li = loop {
                let s = ltab[i];
                if s.0 == c.key {
                    break s.1;
                }
                if s.0 == 0 {
                    break u32::MAX;
                }
                i = (i + 1) & mask;
            };
            if li != u32::MAX {
                ccnt[li as usize] += 1;
            }
            idx.push(li);
        }
        let mut acc = 0u32;
        for c in ccnt.iter_mut() {
            let n = *c;
            *c = acc;
            acc += n;
        }
        let nc = acc as usize;
        let mut cs = std::mem::take(&mut self.cs);
        cs.clear();
        cs.reserve(nc);
        unsafe { cs.set_len(nc) };
        {
            let mut pos: Vec<u32> = ccnt[..np].to_vec();
            for (j, c) in gather(ck_parts, b).enumerate() {
                let li = idx[j];
                if li != u32::MAX {
                    cs[pos[li as usize] as usize] = *c;
                    pos[li as usize] += 1;
                }
            }
        }
        for p in 0..np {
            let ev = &mut es[cnt[p] as usize..cnt[p + 1] as usize];
            if ev.len() > 1 {
                if ev.len() <= 16 {
                    insertion_sort_by_key(ev, |e| e.ts);
                } else {
                    ev.sort_unstable_by_key(|e| e.ts);
                }
            }
            let ck = &mut cs[ccnt[p] as usize..ccnt[p + 1] as usize];
            if ck.len() > 1 {
                ck.sort_unstable_by(|a, c| a.ts.cmp(&c.ts).then_with(|| corpus.bytes(c.cid).cmp(corpus.bytes(a.cid))));
            }
            let (ev, ck) = (&es[cnt[p] as usize..cnt[p + 1] as usize], &cs[ccnt[p] as usize..ccnt[p + 1] as usize]);
            if !ev.is_empty() {
                self.person(pnames[p], ev, ck, ctx);
            }
        }
        // reset the used slots: each person's key is still in its slot
        for &k in &pkeys {
            let mut i = (k as usize) & mask;
            while ltab[i].0 != k {
                i = (i + 1) & mask;
            }
            ltab[i].0 = 0;
        }
        self.pnames = pnames;
        self.pkeys = pkeys;
        self.ltab = ltab;
        self.cnt = cnt;
        self.idx = idx;
        self.es = es;
        self.cs = cs;
        self.ccnt = ccnt;
    }

    fn flush_day(&mut self, name: u64, day: i32, sessions: u32, events: u32, revenue: u64) {
        let mut top = u16::MAX;
        let mut best = 0u64;
        for &c in &self.cat_touched {
            let s = self.cat_sum[c as usize];
            if s > best || (s == best && (c as u16) < top) {
                best = s;
                top = c as u16;
            }
        }
        for &c in &self.cat_touched {
            self.cat_sum[c as usize] = 0;
        }
        self.cat_touched.clear();
        self.rows.push(URow { name, day, sessions, events, revenue, top });
    }

    fn person(&mut self, name: u64, evs: &[Ev], cks: &[Ck], cx: &PCtx) {
        let ndays = cx.ndays;
        self.starts.clear();
        self.runs.clear();
        self.run_of.clear();
        let mut cur_day: i32 = i32::MIN;
        let (mut d_sess, mut d_evs, mut d_rev) = (0u32, 0u32, 0u64);
        let mut cp = 0usize; // clicks with ts <= current session start
        let mut i = 0;
        while i < evs.len() {
            let start = evs[i].ts;
            let mut j = i + 1;
            let mut prev = start;
            while j < evs.len() && evs[j].ts - prev <= GAP {
                prev = evs[j].ts;
                j += 1;
            }
            let day = (cx.t0 + start as i64).div_euclid(DAY) as i32;
            if day != cur_day {
                if d_sess > 0 {
                    self.flush_day(name, cur_day, d_sess, d_evs, d_rev);
                }
                cur_day = day;
                d_sess = 0;
                d_evs = 0;
                d_rev = 0;
                self.runs.push((self.starts.len() as u32, day));
            }
            let mut rev = 0u64;
            let mut purchasing = false;
            for e in &evs[i..j] {
                let price = e.pc & 0x00ff_ffff;
                if price != 0 {
                    purchasing = true;
                    rev += price as u64;
                    let c = (e.pc >> 24) as u8;
                    if self.cat_sum[c as usize] == 0 {
                        self.cat_touched.push(c);
                    }
                    self.cat_sum[c as usize] += price as u64;
                }
            }
            d_sess += 1;
            d_evs += (j - i) as u32;
            d_rev += rev;
            self.run_of.push(self.runs.len() as u32 - 1);
            self.starts.push(start);

            // credit: latest click at or before start, within 7 days; ties -> smallest click_id
            while cp < cks.len() && cks[cp].ts <= start {
                cp += 1;
            }
            let credit = if cp > 0 && cks[cp - 1].ts + WEEK32 >= start { cks[cp - 1].camp } else { cx.organic };
            let di = (day as i64 - cx.day0) as usize;
            let cell = &mut self.camp[credit as usize * ndays + di];
            cell[0] += 1;
            cell[1] += purchasing as u64;
            cell[2] += rev;
            i = j;
        }
        if d_sess > 0 {
            self.flush_day(name, cur_day, d_sess, d_evs, d_rev);
        }

        // assists: per campaign, merged intervals [ts, ts + 7d]; count session starts inside, per day
        if cks.is_empty() {
            return;
        }
        self.by_camp.clear();
        self.by_camp.extend(cks.iter().map(|c| (c.camp, c.ts)));
        self.by_camp.sort_unstable();
        let nsess = self.starts.len();
        let mut k = 0;
        while k < self.by_camp.len() {
            let camp = self.by_camp[k].0;
            let (mut a, mut b) = (self.by_camp[k].1, self.by_camp[k].1 + WEEK32);
            k += 1;
            loop {
                let more = k < self.by_camp.len() && self.by_camp[k].0 == camp;
                if more && self.by_camp[k].1 <= b {
                    b = self.by_camp[k].1 + WEEK32;
                    k += 1;
                    continue;
                }
                let lo = self.starts.partition_point(|&s| s < a);
                let hi = self.starts.partition_point(|&s| s <= b);
                if lo < hi {
                    let (r0, r1) = (self.run_of[lo] as usize, self.run_of[hi - 1] as usize);
                    for r in r0..=r1 {
                        let rs = self.runs[r].0 as usize;
                        let re = if r + 1 < self.runs.len() { self.runs[r + 1].0 as usize } else { nsess };
                        let cnt = hi.min(re) - lo.max(rs);
                        let di = (self.runs[r].1 as i64 - cx.day0) as usize;
                        self.camp[camp as usize * ndays + di][3] += cnt as u64;
                    }
                }
                if !more {
                    break;
                }
                a = self.by_camp[k].1;
                b = a + WEEK32;
                k += 1;
            }
        }
    }
}
