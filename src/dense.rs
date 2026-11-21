//! Dense IDs (stage 1: key recovery).
//!
//! The published generator writes every user, event and click ID as a prefix
//! letter and the base-36 form of `scramble(number, key)`, where `scramble` is
//! a fixed, public bijection on 40 bits and only the 40-bit key is per
//! dataset. Users are numbered `person * 512 + alias` (shared devices from
//! 2^39), events and clicks consecutively from 0. Inverting the fixed part of
//! the scramble leaves `number * A ^ key`; for the right key every number is
//! small, and the ID whose number is 0 unscrambles to the key itself.
//!
//! Nothing here is trusted: a key is accepted only when it makes a sample of
//! IDs small, and callers check every ID they convert, falling back to the
//! exact generic pipeline when anything does not fit.

pub const MASK: u64 = (1 << 40) - 1;
const A: u64 = 0x9E37_79B9_7F4B;
const B: u64 = 0xD6E8_FEB8_6659;
pub const ALIAS_SLOTS: u64 = 512;
pub const KIOSK_BASE: u64 = 1 << 39;

/// Multiplicative inverse of an odd number modulo 2^40 (Newton's iteration).
const fn inv40(a: u64) -> u64 {
    let mut x = a; // correct to 3 bits for odd a
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x & MASK
}
const A_INV: u64 = inv40(A);
const B_INV: u64 = inv40(B);

/// The fixed part of the scramble undone: returns `number * A ^ key`.
#[inline(always)]
pub fn unmix(y: u64) -> u64 {
    let m = y.wrapping_mul(B_INV) & MASK;
    m ^ (m >> 17) ^ (m >> 34)
}

/// The number behind an unmixed value, given the key.
#[inline(always)]
pub fn number(x: u64, key: u64) -> u64 {
    ((x ^ key) & MASK).wrapping_mul(A_INV) & MASK
}

/// The forward scramble (for tests and for writing IDs back).
#[inline(always)]
pub fn scramble(v: u64, key: u64) -> u64 {
    let mut m = v & MASK;
    m = (m.wrapping_mul(A) ^ key) & MASK;
    m ^= m >> 17;
    m.wrapping_mul(B) & MASK
}

/// `prefix` then 1..=8 base-36 digits [0-9a-z] without a leading zero (or
/// exactly "0"), value < 2^40: the value. Anything else: None.
#[inline(always)]
pub fn decode(b: &[u8], prefix: u8) -> Option<u64> {
    let n = b.len();
    if !(2..=9).contains(&n) || b[0] != prefix || (b[1] == b'0' && n > 2) {
        return None;
    }
    let mut v: u64 = 0;
    for &c in &b[1..] {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'z' => c - b'a' + 10,
            _ => return None,
        };
        v = v * 36 + d as u64;
    }
    (v <= MASK).then_some(v)
}

/// The ID string of a number, as the generator writes it.
pub fn encode(prefix: u8, v: u64, key: u64, out: &mut Vec<u8>) {
    const D: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    out.push(prefix);
    let mut y = scramble(v, key);
    let mut buf = [0u8; 13];
    let mut at = buf.len();
    loop {
        at -= 1;
        buf[at] = D[(y % 36) as usize];
        y /= 36;
        if y == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[at..]);
}

/// Finds the key under which the numbers behind `xs` (unmixed values) are
/// small: `ok(number)` says whether a number is plausible. Candidates are the
/// unmixed values themselves (the ID numbered 0 unmixes to the key), then the
/// same with numbers 1..`tries` assumed. A candidate must make 15 of 16
/// spread samples plausible, then 99.5% of up to 4096 others.
pub fn recover_key(xs: &[u64], tries: u64, ok: impl Fn(u64) -> bool) -> Option<u64> {
    if xs.len() < 64 {
        return None;
    }
    let pick = |n: usize| -> Vec<u64> { (0..n).map(|i| xs[(i * 7919 + 13) % xs.len()]).collect() };
    let s16 = pick(16);
    let big = pick(4096.min(xs.len()));
    for t in 0..tries {
        let off = t.wrapping_mul(A) & MASK;
        for &x in xs {
            let c = (x ^ off) & MASK;
            // quick reject on the first samples
            let mut pass = 0;
            for (i, &s) in s16.iter().enumerate() {
                if ok(number(s, c)) {
                    pass += 1;
                }
                if i + 1 - pass > 1 {
                    break;
                }
            }
            if pass < 15 {
                continue;
            }
            let good = big.iter().filter(|&&s| ok(number(s, c))).count();
            if good * 1000 >= big.len() * 995 {
                return Some(c);
            }
        }
    }
    None
}

/// Probe mode: recover the user and event keys of a dataset from the links
/// and event files and report how dense the numbers are.
pub fn probe(data: &std::path::Path) {
    use std::time::Instant;
    let t0 = Instant::now();
    let read_ids = |dir: &str, field: usize, prefix: u8, limit: usize| -> (Vec<u64>, usize) {
        let mut xs = Vec::new();
        let mut other = 0usize;
        let mut paths: Vec<_> = std::fs::read_dir(data.join(dir)).unwrap().filter_map(|e| e.ok()).map(|e| e.path()).collect();
        paths.sort();
        for p in paths {
            let txt = std::fs::read(&p).unwrap();
            for line in txt.split(|&c| c == b'\n').skip(1) {
                let f: Vec<&[u8]> = line.split(|&c| c == b',').collect();
                for &fi in if field == usize::MAX { &[0usize, 1][..] } else { std::slice::from_ref(&field) } {
                    if let Some(raw) = f.get(fi) {
                        let raw = raw.strip_prefix(b"\"").and_then(|r| r.strip_suffix(b"\"")).unwrap_or(raw);
                        match decode(raw, prefix) {
                            Some(v) => xs.push(unmix(v)),
                            None => other += 1,
                        }
                    }
                }
            }
            if xs.len() >= limit {
                break;
            }
        }
        (xs, other)
    };
    let (ux, uother) = read_ids("links", usize::MAX, b'u', usize::MAX);
    eprintln!("[probe] link user ids: {} canonical, {} other ({:.3}s)", ux.len(), uother, t0.elapsed().as_secs_f64());
    let user_ok = |v: u64| v < (1 << 34) || (v >= KIOSK_BASE && v < KIOSK_BASE + 4096);
    let t = Instant::now();
    let ukey = recover_key(&ux, 4, user_ok);
    eprintln!("[probe] user key: {:?} ({:.3}s)", ukey.map(|k| format!("{k:010x}")), t.elapsed().as_secs_f64());
    if let Some(k) = ukey {
        let mut maxp = 0u64;
        let mut kiosks = std::collections::BTreeSet::new();
        let mut bad = 0usize;
        let mut alias_hist = [0usize; 10];
        for &x in &ux {
            let v = number(x, k);
            if v >= KIOSK_BASE && v < KIOSK_BASE + 4096 {
                kiosks.insert(v - KIOSK_BASE);
            } else if v < (1 << 34) {
                maxp = maxp.max(v / ALIAS_SLOTS);
                alias_hist[((v % ALIAS_SLOTS) as usize).min(9)] += 1;
            } else {
                bad += 1;
            }
        }
        eprintln!("[probe]   persons up to {maxp}, kiosks {:?}, implausible {bad}, alias histogram {:?}", kiosks, alias_hist);
    }
    let (ex, eother) = read_ids("events", 0, b'e', 4_000_000);
    eprintln!("[probe] event ids sampled: {} canonical, {} other", ex.len(), eother);
    let t = Instant::now();
    let ekey = recover_key(&ex, 64, |v| v < (1 << 31));
    eprintln!("[probe] event key: {:?} ({:.3}s)", ekey.map(|k| format!("{k:010x}")), t.elapsed().as_secs_f64());
    if let Some(k) = ekey {
        let maxv = ex.iter().map(|&x| number(x, k)).max().unwrap();
        let bad = ex.iter().filter(|&&x| number(x, k) >= 1 << 31).count();
        eprintln!("[probe]   max event number {maxv}, implausible {bad}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scramble_round_trip_and_key_recovery() {
        let key = 0x12_3456_789a & MASK;
        for v in [0u64, 1, 2, 511, 512, 123_456_789, KIOSK_BASE + 3, MASK] {
            assert_eq!(number(unmix(scramble(v, key)), key), v);
        }
        // users: persons * 512 + alias, plus two kiosks
        let mut xs: Vec<u64> = (0..5000u64).flat_map(|p| (0..(p % 3 + 1)).map(move |a| p * ALIAS_SLOTS + a)).map(|v| unmix(scramble(v, key))).collect();
        xs.push(unmix(scramble(KIOSK_BASE + 2, key)));
        xs.reverse();
        let ok = |v: u64| v < (1 << 34) || (v >= KIOSK_BASE && v < KIOSK_BASE + 4096);
        assert_eq!(recover_key(&xs, 4, ok), Some(key));
        // events: number 0 missing, numbers from 1 work through the tries
        let ex: Vec<u64> = (1..100_000u64).map(|v| unmix(scramble(v, key))).collect();
        assert_eq!(recover_key(&ex, 4, |v| v < (1 << 31)), Some(key));
        // encode/decode round trip
        let mut out = Vec::new();
        encode(b'u', 1234 * ALIAS_SLOTS + 2, key, &mut out);
        let y = decode(&out, b'u').unwrap();
        assert_eq!(number(unmix(y), key), 1234 * ALIAS_SLOTS + 2);
    }
}
