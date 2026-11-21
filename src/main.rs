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
/// An event copy as parsed. `ts == INVALID` marks a copy that fails
/// validation (it still takes part in dedup).
#[derive(Clone, Copy, Default)]
struct ERec {
    eh: u64,
    uh: u64,
    uref: u64,
    ts: u32,     // ms since t0 = window_start - 7 days
    ingest: i32, // seconds since t0, saturated
    pc: u32,     // price (24 bits) | category << 24; price 0 = not a purchase
}

/// A surviving event: key is the user_id hash, then the person hash once
/// resolved; r is the user_id ref, then the person's name ref.
#[derive(Clone, Copy)]
struct Ev {
    key: u64,
    r: u64,
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
    let (people, ep_valid, ep_ids, cp) = std::thread::scope(|scope| {
        let links = scope.spawn(links_job);
        // ================================================================ events: parse
        // Valid copies go to their user_id bucket in full; every copy also sends
        // (event_id hash, ingest) to its event_id bucket for duplicate detection.
        let ep = par_fin(
            threads,
            event_chunks.len(),
            || (Parts::<ERec>::new(NB), Parts::<(u64, i32)>::new(NB)),
            |(uk, dk), i| {
                let c = event_chunks[i];
                let data: &[u8] = &corpus.maps[c.file as usize];
                parse::for_rows::<7>(data, c.start, c.end, |f| {
                    if f[0].is_empty() {
                        return;
                    }
                    let eh = hash(f[0]);
                    let ingest_abs = parse::timestamp(f[3]).unwrap_or(i64::MAX);
                    let ingest = ((ingest_abs.saturating_sub(t0)) / 1000).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                    dk.push(bucket(eh), (eh, ingest));
                    if f[1].is_empty() || f[5].is_empty() {
                        return;
                    }
                    let Some(ts) = parse::timestamp(f[2]) else { return };
                    if ts < ws || ts >= we {
                        return;
                    }
                    let mut price = 0u32;
                    match f[4] {
                        b"view" | b"cart" => {}
                        b"purchase" => match parse::price(f[6]) {
                            Some(p) => price = p,
                            None => return,
                        },
                        _ => return,
                    }
                    if ingest_abs.saturating_sub(ts) > LATE {
                        return;
                    }
                    let cat = products_lookup(f[5]);
                    let uh = hash(f[1]);
                    uk.push(bucket(uh), ERec { eh, uh, uref: corpus.rf(c.file, f[1]), ts: (ts - t0) as u32, ingest, pc: price | (cat << 24) });
                });
            },
            |(uk, dk)| { uk.finish(); dk.finish(); },
        );
        let (ep_valid, ep_ids): (Vec<_>, Vec<_>) = ep.into_iter().unzip();
        lap("events parsed");

        // ================================================================ clicks: parse
        let cp = par_fin(
            threads,
            click_chunks.len(),
            || Parts::<Ck>::new(NB),
            |bk, i| {
                let c = click_chunks[i];
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
            },
            |bk| bk.finish(),
        );
        lap("clicks parsed");
        (links.join().unwrap(), ep_valid, ep_ids, cp)
    });

    // ================================================================ duplicate event_ids
    // Per event_id bucket (second-level groups keep tables tiny): every
    // event_id seen more than once, with its smallest ingest.
    let dups_found = par(
        threads,
        NB,
        || {
            (
                Vec::<(usize, Vec<(u64, i32)>)>::new(),
                (0..256).map(|_| Vec::<(u64, i32)>::new()).collect::<Vec<_>>(),
                Table::<(i32, u32)>::with_capacity(4096),
            )
        },
        |(acc, groups, tab), b| {
            for &(eh, ing) in gather(&ep_ids, b) {
                groups[((eh >> 48) & 255) as usize].push((eh, ing));
            }
            let mut found = Vec::new();
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
                for &(eh, ing) in grp.iter() {
                    let (v, new) = tab.slot(eh);
                    if new {
                        *v = (ing, 1);
                    } else {
                        v.0 = v.0.min(ing);
                        v.1 += 1;
                    }
                }
                for s in &tab.slots {
                    if s.0 != 0 && s.1 .1 > 1 {
                        found.push((s.0, s.1 .0));
                    }
                }
                grp.clear();
            }
            acc.push((b, found));
        },
    );
    drop(ep_ids);
    let mut dup_lists: Vec<Vec<(u64, i32)>> = (0..NB).map(|_| Vec::new()).collect();
    for (b, v) in dups_found.into_iter().flat_map(|s| s.0) {
        dup_lists[b] = v;
    }
    let ndups: usize = dup_lists.iter().map(|v| v.len()).sum();
    let dup_tabs: Vec<Table<i32>> = dup_lists
        .iter()
        .map(|v| {
            let mut t = Table::with_capacity(v.len());
            for &(eh, ing) in v {
                t.insert(eh, ing);
            }
            t
        })
        .collect();
    // A one-bit-per-hash filter in front of dup_tabs, small enough for L2.
    let bloom_bits = (ndups * 16).next_power_of_two().clamp(1 << 16, 1 << 27);
    let bloom_mask = (bloom_bits - 1) as u64;
    let mut bloom = vec![0u64; bloom_bits / 64];
    for v in &dup_lists {
        for &(eh, _) in v {
            let k = (eh >> 20) & bloom_mask;
            bloom[(k >> 6) as usize] |= 1 << (k & 63);
        }
    }
    drop(dup_lists);
    lap(&format!("duplicates: {ndups} event_ids"));

    // ================================================================ resolve people per user_id bucket
    // Drops the losing copies of duplicated events (identical winning copies
    // share a user_id, so they meet here and only one is kept).
    let rp = par_fin(
        threads,
        NB,
        || (Parts::<Ev>::new(NB), Parts::<Ck>::new(NB), std::collections::HashSet::<u64>::new()),
        |(eo, co, seen), b| {
            let tab = &people[b];
            seen.clear();
            for e in gather(&ep_valid, b) {
                let k = (e.eh >> 20) & bloom_mask;
                if bloom[(k >> 6) as usize] & (1 << (k & 63)) != 0 {
                    if let Some(min) = dup_tabs[bucket(e.eh)].get(e.eh) {
                        if e.ingest != min {
                            continue;
                        }
                        if !seen.insert(e.eh) {
                            continue;
                        }
                    }
                }
                let (key, r) = tab.get(e.uh).unwrap_or((e.uh, e.uref));
                eo.push(bucket(key), Ev { key, r, ts: e.ts, pc: e.pc });
            }
            for c in gather(&cp, b) {
                let key = tab.get(c.key).map_or(c.key, |x| x.0);
                co.push(bucket(key), Ck { key, ..*c });
            }
        },
        |(eo, co, _)| { eo.finish(); co.finish(); },
    );
    drop(ep_valid);
    drop(cp);
    let (ev_parts, ck_parts): (Vec<_>, Vec<_>) = rp.into_iter().map(|(e, c, _)| (e, c)).unzip();
    lap("resolved");

    // ================================================================ per person bucket
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let ncamp = campaigns.len() + 1;
    let ncats = cats.len();
    let ctx = PCtx { t0, day0, ndays, organic, ws_rel, we_rel };
    let results = par(
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
            eg: Vec::new(),
            cg: Vec::new(),
        },
        |w, b| {
            // Second-level groups by person hash, so each sort runs in cache.
            if w.eg.is_empty() {
                w.eg = (0..256).map(|_| Vec::new()).collect();
                w.cg = (0..256).map(|_| Vec::new()).collect();
            }
            for e in gather(&ev_parts, b) {
                w.eg[((e.key >> 48) & 255) as usize].push(*e);
            }
            for c in gather(&ck_parts, b) {
                w.cg[((c.key >> 48) & 255) as usize].push(*c);
            }
            for g in 0..256 {
                let mut evs = std::mem::take(&mut w.eg[g]);
                let mut cks = std::mem::take(&mut w.cg[g]);
                if !evs.is_empty() {
                    w.group(&mut evs, &mut cks, &ctx, corpus);
                }
                evs.clear();
                cks.clear();
                w.eg[g] = evs;
                w.cg[g] = cks;
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

    let cats: &'static [Vec<u8>] = Box::leak(cats.into_boxed_slice());
    let octx = out::Ctx { corpus, cats, campaigns: &campaigns, organic };
    if csv_out {
        out::write_csv(&out_dir, &octx, &user_parts, &crows);
    } else {
        out::write_parquet(&out_dir, &octx, &user_parts, &crows, threads);
    }
    lap(&format!("wrote {nrows} user rows, {} campaign rows", crows.len()));
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
    eg: Vec<Vec<Ev>>,
    cg: Vec<Vec<Ck>>,
}

impl Worker {
    fn group(&mut self, evs: &mut [Ev], cks: &mut [Ck], ctx: &PCtx, corpus: &Corpus) {
        evs.sort_unstable_by_key(|e| (e.key, e.ts));
        cks.sort_unstable_by(|a, c| {
            a.key
                .cmp(&c.key)
                .then(a.ts.cmp(&c.ts))
                .then_with(|| corpus.bytes(c.cid).cmp(corpus.bytes(a.cid)))
        });
        let mut ci = 0;
        let mut i = 0;
        while i < evs.len() {
            let p = evs[i].key;
            let mut j = i + 1;
            while j < evs.len() && evs[j].key == p {
                j += 1;
            }
            while ci < cks.len() && cks[ci].key < p {
                ci += 1;
            }
            let mut cj = ci;
            while cj < cks.len() && cks[cj].key == p {
                cj += 1;
            }
            self.person(&evs[i..j], &cks[ci..cj], ctx);
            ci = cj;
            i = j;
        }
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

    fn person(&mut self, evs: &[Ev], cks: &[Ck], cx: &PCtx) {
        let name = evs[0].r;
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
