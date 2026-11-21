//! Shopping Sessions: the fast pipeline.
//!
//! Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
//! With OUT_FORMAT=csv it writes $OUTPUT_DIR/{user_daily,campaign_daily}.csv
//! instead (the fixtures' layout), for diffing against the reference.

mod out;
mod parse;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use memmap2::Mmap;
use parse::{split, DAY};
use xxhash_rust::xxh3::xxh3_64;

const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const LATE: i64 = DAY;
const CHUNK: usize = 4 << 20;
const BUCKET_BITS: u32 = 8;
const NB: usize = 1 << BUCKET_BITS;

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

#[inline(always)]
fn bucket(h: u64) -> usize {
    (h >> (64 - BUCKET_BITS)) as usize
}

/// Open addressing u64 -> u32, key 0 is empty. Keys are already hashes.
pub struct Table {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
}

impl Table {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        Table { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1 }
    }
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<u32> {
        let mut i = (k as usize) & self.mask;
        loop {
            let s = self.keys[i];
            if s == k {
                return Some(self.vals[i]);
            }
            if s == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Returns the slot index and whether the key was new.
    #[inline(always)]
    pub fn slot(&mut self, k: u64) -> (usize, bool) {
        let mut i = (k as usize) & self.mask;
        loop {
            let s = self.keys[i];
            if s == k {
                return (i, false);
            }
            if s == 0 {
                self.keys[i] = k;
                return (i, true);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn insert(&mut self, k: u64, v: u32) {
        let (i, _) = self.slot(k);
        self.vals[i] = v;
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
}

#[derive(Clone, Copy)]
struct Chunk {
    file: u32,
    start: usize,
    end: usize,
}

struct Kind {
    chunks: Vec<Chunk>,
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

fn add_files(maps: &mut Vec<Mmap>, bases: &mut Vec<u64>, next: &mut u64, paths: &[PathBuf]) -> Kind {
    let mut chunks = Vec::new();
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
        // The first line of every file is its header.
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
    Kind { chunks }
}

/// Runs `f(state, chunk)` over all chunks on `threads` threads; returns the states.
fn par_chunks<S: Send, F>(threads: usize, chunks: &[Chunk], init: impl Fn() -> S + Sync, f: F) -> Vec<S>
where
    F: Fn(&mut S, Chunk) + Sync,
{
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= chunks.len() {
                            break;
                        }
                        f(&mut st, chunks[i]);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// Runs `f(state, i)` for i in 0..n on `threads` threads; returns the states.
fn par_range<S: Send, F>(threads: usize, n: usize, init: impl Fn() -> S + Sync, f: F) -> Vec<S>
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

#[inline(always)]
fn for_lines(data: &[u8], c: Chunk, mut f: impl FnMut(&[u8])) {
    let mut p = c.start;
    let end = c.end;
    while p < end {
        let nl = match memchr::memchr(b'\n', &data[p..end]) {
            Some(i) => p + i,
            None => end,
        };
        f(&data[p..nl]);
        p = nl + 1;
    }
}

// ------------------------------------------------------------------ records
/// An event copy as parsed. Only `eh` and `ingest` matter for an invalid one.
#[derive(Clone, Copy)]
struct ERec {
    eh: u64,
    ingest: i64,
    person: u64,
    ts: i64,
    name: u64,
    price: u32,
    cat: u16,
    flags: u8, // 1 = valid (incl. not late), 2 = purchase
}

/// A surviving event, keyed by person.
#[derive(Clone, Copy)]
struct PRec {
    person: u64,
    ts: i64,
    name: u64,
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct CRec {
    person: u64,
    ts: i64,
    cid: u64,
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

/// Person lookup built from links: ID hash -> (name hash, name ref).
struct People {
    table: Table,
    name_hash: Vec<u64>,
    name_ref: Vec<u64>,
}

impl People {
    #[inline(always)]
    fn resolve(&self, h: u64, r: u64) -> (u64, u64) {
        match self.table.get(h) {
            Some(i) => (self.name_hash[i as usize], self.name_ref[i as usize]),
            None => (h, r),
        }
    }
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
    let t0 = Instant::now();
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let csv_out = std::env::var("OUT_FORMAT").map(|v| v == "csv").unwrap_or(false);
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from))
        .max(1);
    let lap = |what: &str| eprintln!("[{:>8.3}s] {what}", t0.elapsed().as_secs_f64());

    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_time(&manifest, "window_start");
    let we = manifest_time(&manifest, "window_end");
    let cws = ws - WEEK;

    // ---- products -> category index (sorted, "unknown" included), campaigns
    let products = read_small(&data_dir.join("products.csv"));
    let mut cats: Vec<Vec<u8>> = products.iter().map(|r| r[1].clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let unknown_cat = cats.binary_search(&b"unknown".to_vec()).unwrap() as u16;
    let mut prod_tab = Table::with_capacity(products.len());
    for r in &products {
        let c = cats.binary_search(&r[1]).unwrap() as u32;
        prod_tab.insert(hash(&r[0]), c);
    }
    let campaigns = read_small(&data_dir.join("campaigns.csv"));
    let mut camp_tab = Table::with_capacity(campaigns.len());
    for (i, r) in campaigns.iter().enumerate() {
        camp_tab.insert(hash(&r[0]), i as u32);
    }
    let organic = campaigns.len() as u32;

    // ---- map inputs
    let mut maps = Vec::new();
    let mut bases = Vec::new();
    let mut next = 0u64;
    let links_k = add_files(&mut maps, &mut bases, &mut next, &list_csv(&data_dir.join("links")));
    let events_k = add_files(&mut maps, &mut bases, &mut next, &list_csv(&data_dir.join("events")));
    let clicks_k = add_files(&mut maps, &mut bases, &mut next, &list_csv(&data_dir.join("clicks")));
    let corpus = Corpus { maps, bases };
    let rf = |file: u32, field: &[u8]| -> u64 {
        let m = &corpus.maps[file as usize];
        let off = corpus.bases[file as usize] + (field.as_ptr() as usize - m.as_ptr() as usize) as u64;
        (off << 16) | (field.len().min(0xffff) as u64)
    };
    lap("mapped");

    // ---- links
    let link_parts = par_chunks(threads, &links_k.chunks, Vec::<[u64; 4]>::new, |v, c| {
        let data: &[u8] = &corpus.maps[c.file as usize];
        for_lines(data, c, |line| {
            let mut f = [&b""[..]; 2];
            if split(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                v.push([hash(f[0]), rf(c.file, f[0]), hash(f[1]), rf(c.file, f[1])]);
            }
        });
    });
    let nlinks: usize = link_parts.iter().map(|v| v.len()).sum();
    let mut idx = Table::with_capacity(nlinks * 2);
    let mut ids_h: Vec<u64> = Vec::new();
    let mut ids_r: Vec<u64> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
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
    for part in &link_parts {
        for &[ha, ra, hb, rb] in part {
            let mut get = |h: u64, r: u64| -> u32 {
                let (s, new) = idx.slot(h);
                if new {
                    let i = ids_h.len() as u32;
                    idx.vals[s] = i;
                    ids_h.push(h);
                    ids_r.push(r);
                    parent.push(i);
                    i
                } else {
                    idx.vals[s]
                }
            };
            let a = get(ha, ra);
            let b = get(hb, rb);
            let (x, y) = (find(&mut parent, a), find(&mut parent, b));
            if x != y {
                parent[y as usize] = x;
            }
        }
    }
    drop(link_parts);
    let n_ids = ids_h.len();
    // best[root] = member with the byte-wise smallest ID
    let mut best: Vec<u32> = (0..n_ids as u32).collect();
    for i in 0..n_ids as u32 {
        let r = find(&mut parent, i);
        if r != i && corpus.bytes(ids_r[i as usize]) < corpus.bytes(ids_r[best[r as usize] as usize]) {
            best[r as usize] = i;
        }
    }
    let mut name_hash = vec![0u64; n_ids];
    let mut name_ref = vec![0u64; n_ids];
    for i in 0..n_ids {
        let b = best[parent[i] as usize] as usize; // parent[i] is the root after find above
        name_hash[i] = ids_h[b];
        name_ref[i] = ids_r[b];
    }
    let people = People { table: idx, name_hash, name_ref };
    drop((ids_h, ids_r, best));
    lap(&format!("links: {nlinks} lines, {n_ids} ids"));

    // ---- events: parse, validate each copy, partition by event_id hash
    let ev_parts = par_chunks(
        threads,
        &events_k.chunks,
        || (0..NB).map(|_| Vec::<ERec>::new()).collect::<Vec<_>>(),
        |bk, c| {
            let data: &[u8] = &corpus.maps[c.file as usize];
            for_lines(data, c, |line| {
                let mut f = [&b""[..]; 7];
                if !split(line, &mut f) || f[0].is_empty() {
                    return;
                }
                let eh = hash(f[0]);
                let ingest = parse::timestamp(f[3]).unwrap_or(i64::MAX);
                let mut r = ERec { eh, ingest, person: 0, ts: 0, name: 0, price: 0, cat: 0, flags: 0 };
                'v: {
                    if f[1].is_empty() || f[5].is_empty() {
                        break 'v;
                    }
                    let Some(ts) = parse::timestamp(f[2]) else { break 'v };
                    if ts < ws || ts >= we {
                        break 'v;
                    }
                    let purchase = match f[4] {
                        b"view" | b"cart" => false,
                        b"purchase" => true,
                        _ => break 'v,
                    };
                    if purchase {
                        let Some(p) = parse::price(f[6]) else { break 'v };
                        r.price = p;
                    }
                    if ingest.saturating_sub(ts) > LATE {
                        break 'v;
                    }
                    let (person, name) = people.resolve(hash(f[1]), rf(c.file, f[1]));
                    r.person = person;
                    r.name = name;
                    r.ts = ts;
                    r.cat = prod_tab.get(hash(f[5])).map_or(unknown_cat, |c| c as u16);
                    r.flags = 1 | if purchase { 2 } else { 0 };
                }
                bk[bucket(eh)].push(r);
            });
        },
    );
    lap("events parsed");

    // ---- clicks: parse, validate, partition by person
    let ck_parts = par_chunks(
        threads,
        &clicks_k.chunks,
        || (0..NB).map(|_| Vec::<CRec>::new()).collect::<Vec<_>>(),
        |bk, c| {
            let data: &[u8] = &corpus.maps[c.file as usize];
            for_lines(data, c, |line| {
                let mut f = [&b""[..]; 4];
                if !split(line, &mut f) || f[0].is_empty() || f[1].is_empty() || f[2].is_empty() {
                    return;
                }
                let Some(camp) = camp_tab.get(hash(f[2])) else { return };
                let Some(ts) = parse::timestamp(f[3]) else { return };
                if ts < cws || ts >= we {
                    return;
                }
                let (person, _) = people.resolve(hash(f[1]), 0);
                bk[bucket(person)].push(CRec { person, ts, cid: rf(c.file, f[0]), camp });
            });
        },
    );
    lap("clicks parsed");

    // ---- dedup per event_id bucket, survivors partitioned by person
    let ded = par_range(
        threads,
        NB,
        || (0..NB).map(|_| Vec::<PRec>::new()).collect::<Vec<_>>(),
        |out, b| {
            let n: usize = ev_parts.iter().map(|t| t[b].len()).sum();
            let mut tab = Table::with_capacity(n);
            let mut kept: Vec<ERec> = Vec::with_capacity(n);
            for t in &ev_parts {
                for r in &t[b] {
                    let (s, new) = tab.slot(r.eh);
                    if new {
                        tab.vals[s] = kept.len() as u32;
                        kept.push(*r);
                    } else {
                        let k = &mut kept[tab.vals[s] as usize];
                        if r.ingest < k.ingest {
                            *k = *r;
                        }
                    }
                }
            }
            for r in &kept {
                if r.flags & 1 != 0 {
                    out[bucket(r.person)].push(PRec {
                        person: r.person,
                        ts: r.ts,
                        name: r.name,
                        price: r.price,
                        cat: r.cat,
                        purchase: r.flags & 2 != 0,
                    });
                }
            }
        },
    );
    drop(ev_parts);
    lap("deduped");

    // ---- per person bucket: sessions, attribution, aggregates
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let ncamp = campaigns.len() + 1;
    let ncats = cats.len();
    let results = par_range(
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
        },
        |w, b| {
            let mut evs: Vec<PRec> = Vec::with_capacity(ded.iter().map(|t| t[b].len()).sum());
            for t in &ded {
                evs.extend_from_slice(&t[b]);
            }
            evs.sort_unstable_by_key(|e| (e.person, e.ts));
            let mut cks: Vec<CRec> = Vec::with_capacity(ck_parts.iter().map(|t| t[b].len()).sum());
            for t in &ck_parts {
                cks.extend_from_slice(&t[b]);
            }
            cks.sort_unstable_by(|a, c| {
                a.person
                    .cmp(&c.person)
                    .then(a.ts.cmp(&c.ts))
                    .then_with(|| corpus.bytes(c.cid).cmp(corpus.bytes(a.cid)))
            });
            let mut ci = 0;
            let mut i = 0;
            while i < evs.len() {
                let p = evs[i].person;
                let mut j = i + 1;
                while j < evs.len() && evs[j].person == p {
                    j += 1;
                }
                while ci < cks.len() && cks[ci].person < p {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < cks.len() && cks[cj].person == p {
                    cj += 1;
                }
                w.person(&evs[i..j], &cks[ci..cj], day0, ndays, organic);
                ci = cj;
                i = j;
            }
        },
    );
    drop(ded);
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

    let ctx = out::Ctx { corpus: &corpus, cats: &cats, campaigns: &campaigns, organic };
    if csv_out {
        out::write_csv(&out_dir, &ctx, &user_parts, &crows);
    } else {
        out::write_parquet(&out_dir, &ctx, &user_parts, &crows, threads);
    }
    lap(&format!("wrote {nrows} user rows, {} campaign rows", crows.len()));
}

struct Worker {
    rows: Vec<URow>,
    camp: Vec<[u64; 4]>,
    cat_sum: Vec<u64>,
    cat_touched: Vec<u16>,
    starts: Vec<i64>,
    runs: Vec<(u32, i32)>, // (first session index, day)
    run_of: Vec<u32>,
    by_camp: Vec<(u32, i64)>,
}

impl Worker {
    fn flush_day(&mut self, name: u64, day: i32, sessions: u32, events: u32, revenue: u64) {
        let mut top = u16::MAX;
        let mut best = 0u64;
        for &c in &self.cat_touched {
            let s = self.cat_sum[c as usize];
            if s > best || (s == best && c < top) {
                best = s;
                top = c;
            }
        }
        for &c in &self.cat_touched {
            self.cat_sum[c as usize] = 0;
        }
        self.cat_touched.clear();
        self.rows.push(URow { name, day, sessions, events, revenue, top });
    }

    fn person(&mut self, evs: &[PRec], cks: &[CRec], day0: i64, ndays: usize, organic: u32) {
        let name = evs[0].name;
        self.starts.clear();
        self.runs.clear();
        self.run_of.clear();
        // day accumulators
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
            // session = evs[i..j]
            let day = start.div_euclid(DAY) as i32;
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
                if e.purchase {
                    purchasing = true;
                    rev += e.price as u64;
                    let c = e.cat as usize;
                    if self.cat_sum[c] == 0 {
                        self.cat_touched.push(e.cat);
                    }
                    self.cat_sum[c] += e.price as u64;
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
            let credit = if cp > 0 && cks[cp - 1].ts >= start - WEEK { cks[cp - 1].camp } else { organic };
            let di = (day as i64 - day0) as usize;
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
            let (mut a, mut b) = (self.by_camp[k].1, self.by_camp[k].1 + WEEK);
            k += 1;
            loop {
                let more = k < self.by_camp.len() && self.by_camp[k].0 == camp;
                if more && self.by_camp[k].1 <= b {
                    b = self.by_camp[k].1 + WEEK;
                    k += 1;
                    continue;
                }
                // flush [a, b]
                let lo = self.starts.partition_point(|&s| s < a);
                let hi = self.starts.partition_point(|&s| s <= b);
                if lo < hi {
                    let (r0, r1) = (self.run_of[lo] as usize, self.run_of[hi - 1] as usize);
                    for r in r0..=r1 {
                        let rs = self.runs[r].0 as usize;
                        let re = if r + 1 < self.runs.len() { self.runs[r + 1].0 as usize } else { nsess };
                        let cnt = hi.min(re) - lo.max(rs);
                        let di = (self.runs[r].1 as i64 - day0) as usize;
                        self.camp[camp as usize * ndays + di][3] += cnt as u64;
                    }
                }
                if !more {
                    break;
                }
                a = self.by_camp[k].1;
                b = a + WEEK;
                k += 1;
            }
        }
    }
}
