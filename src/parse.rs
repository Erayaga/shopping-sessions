//! The CSV dialect, timestamps and prices, exactly as rules.md defines them.

pub const SECOND: i64 = 1_000;
pub const DAY: i64 = 86_400_000;

/// Splits one line (without its `\n`) into exactly N fields, removing the
/// quotes around a whole field. Returns false for a malformed line: the wrong
/// field count, or a quote anywhere except around a whole field. An empty
/// field (also `""`) comes back as an empty slice, meaning null.
#[inline(always)]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    let mut n = 0;
    let mut start = 0;
    loop {
        let mut j = start;
        let mut quote = false;
        while j < len {
            let c = line[j];
            if c == b',' {
                break;
            }
            quote |= c == b'"';
            j += 1;
        }
        if n == N {
            return false;
        }
        let mut f = &line[start..j];
        if quote {
            let l = f.len();
            if l < 2 || f[0] != b'"' || f[l - 1] != b'"' || memchr::memchr(b'"', &f[1..l - 1]).is_some() {
                return false;
            }
            f = &f[1..l - 1];
        }
        out[n] = f;
        n += 1;
        if j == len {
            break;
        }
        start = j + 1;
    }
    n == N
}

/// Comma, newline and quote bitmasks of the 128 bytes at p.
#[cfg(target_feature = "avx2")]
#[inline(always)]
#[allow(dead_code)] // reference for tests
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    let comma = _mm256_set1_epi8(b',' as i8);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let quote = _mm256_set1_epi8(b'"' as i8);
    let (mut c, mut n, mut q) = (0u128, 0u128, 0u128);
    for k in 0..4 {
        let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
        c |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, comma)) as u32 as u128) << (32 * k);
        n |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nl)) as u32 as u128) << (32 * k);
        q |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, quote)) as u32 as u128) << (32 * k);
        if n != 0 {
            break;
        }
    }
    (c, n, q)
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
#[allow(dead_code)]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    let (mut c, mut n, mut q) = (0u128, 0u128, 0u128);
    for i in 0..128 {
        let b = *p.add(i);
        c |= ((b == b',') as u128) << i;
        n |= ((b == b'\n') as u128) << i;
        q |= ((b == b'"') as u128) << i;
        if b == b'\n' {
            break;
        }
    }
    (c, n, q)
}

/// Bits a..b (b exclusive, b <= 127).
#[inline(always)]
#[allow(dead_code)] // reference for tests
fn range(a: usize, b: usize) -> u128 {
    ((1u128 << b) - 1) & !((1u128 << a) - 1)
}

/// Splits the line that starts at `start` (it ends at the next '\n' or at
/// `end`). Returns where the next line starts and whether `out` holds a
/// well-formed row. Same rules as `split`.
#[inline(always)]
#[allow(dead_code)] // reference for tests
pub fn next_line<'a, const N: usize>(data: &'a [u8], start: usize, end: usize, out: &mut [&'a [u8]; N]) -> (usize, bool) {
    if start + 128 <= data.len() {
        let (cm, nm, qm) = unsafe { masks128(data.as_ptr().add(start)) };
        if nm != 0 {
            let nl = nm.trailing_zeros() as usize;
            if start + nl < end {
                let next = start + nl + 1;
                let valid = (1u128 << nl) - 1;
                let mut commas = cm & valid;
                if commas.count_ones() as usize != N - 1 {
                    return (next, false);
                }
                let quotes = qm & valid;
                let mut a = 0usize;
                for (i, slot) in out.iter_mut().enumerate() {
                    let b = if i + 1 < N {
                        let t = commas.trailing_zeros() as usize;
                        commas &= commas - 1;
                        t
                    } else {
                        nl
                    };
                    let (mut fa, mut fb) = (a, b);
                    if quotes != 0 {
                        let fq = quotes & range(a, b);
                        if fq != 0 {
                            if b < a + 2 || fq != (1u128 << a) | (1u128 << (b - 1)) {
                                return (next, false);
                            }
                            fa += 1;
                            fb -= 1;
                        }
                    }
                    *slot = &data[start + fa..start + fb];
                    a = b + 1;
                }
                return (next, true);
            }
        }
    }
    let nl = match memchr::memchr(b'\n', &data[start..end]) {
        Some(i) => start + i,
        None => end,
    };
    (nl + 1, split(&data[start..nl], out))
}

/// Calls f for every well-formed row among the lines in data[start..end].
#[inline(always)]
#[allow(dead_code)] // reference for tests
pub fn for_rows_ref<'a, const N: usize>(data: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; N])) {
    let mut out: [&'a [u8]; N] = [&data[..0]; N];
    let mut p = start;
    while p < end {
        let (next, ok) = next_line(data, p, end, &mut out);
        if ok {
            f(&out);
        }
        p = next;
    }
}

#[inline(always)]
fn digit(c: u8) -> Option<i64> {
    let d = c.wrapping_sub(b'0');
    if d < 10 {
        Some(d as i64)
    } else {
        None
    }
}

#[inline(always)]
fn two(b: &[u8], i: usize) -> Option<i64> {
    Some(digit(b[i])? * 10 + digit(b[i + 1])?)
}

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

const TABLE_Y0: i64 = 1900;
const TABLE_YEARS: i64 = 300;
/// (days since epoch of the 1st, days in the month) for 1900-01 .. 2199-12.
static MONTHS: [(i32, u8); (TABLE_YEARS * 12) as usize] = {
    let mut t = [(0i32, 0u8); (TABLE_YEARS * 12) as usize];
    let mut i = 0;
    while i < t.len() {
        let y = TABLE_Y0 + (i / 12) as i64;
        let m = (i % 12) as i64 + 1;
        // days_from_civil(y, m, 1), written out for const evaluation
        let yy = if m <= 2 { y - 1 } else { y };
        let era = yy.div_euclid(400);
        let yoe = yy - era * 400;
        let doy = (153 * ((m + 9) % 12) + 2) / 5;
        let start = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let dim = match m {
            2 => if leap { 29 } else { 28 },
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        t[i] = (start as i32, dim);
        i += 1;
    }
    t
};

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        2 => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// `YYYY-MM-DDTHH:MM:SS` in b[..19], as epoch ms.
#[inline(always)]
fn datetime_ref(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = two(b, 0)? * 100 + two(b, 2)?;
    let mo = two(b, 5)?;
    let d = two(b, 8)?;
    let h = two(b, 11)?;
    let mi = two(b, 14)?;
    let s = two(b, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    if (TABLE_Y0..TABLE_Y0 + TABLE_YEARS).contains(&y) {
        let (start, dim) = MONTHS[((y - TABLE_Y0) * 12 + mo - 1) as usize];
        if d > dim as i64 {
            return None;
        }
        return Some((((start as i64 + d - 1) * 24 + h) * 60 + mi) * 60_000 + s * SECOND);
    }
    if d > days_in_month(y, mo) {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * SECOND)
}

/// Any of the three forms, as epoch ms. None for a malformed value; an F3
/// value too big for i64 is also None, which is just as outside every window.
#[inline]
pub fn timestamp_ref(b: &[u8]) -> Option<i64> {
    let len = b.len();
    if len == 0 {
        return None;
    }
    if len == 20 && b[19] == b'Z' {
        return datetime_ref(b);
    }
    if len == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base = datetime_ref(b)?;
        let oh = two(b, 20)?;
        let om = two(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if b[19] == b'+' { base - off } else { base + off });
    }
    // F3: digits only, leading zeros allowed.
    let mut v: i64 = 0;
    let mut sig = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
        }
        if sig > 18 {
            // Can't fit or is far outside any window; keep checking it's all digits.
            if !b.iter().all(|c| c.is_ascii_digit()) {
                return None;
            }
            return Some(i64::MAX);
        }
        v = v * 10 + d as i64;
    }
    Some(v)
}

/// A purchase price: digits only, 1 to 10,000,000.
#[inline]
pub fn price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut sig = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
        }
        if sig > 9 {
            v = u64::MAX;
        } else {
            v = v * 10 + d as u64;
        }
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps() {
        let f1 = timestamp(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(timestamp(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(timestamp(b"1791071890000"), Some(f1));
        assert_eq!(timestamp(b"0001791071890000"), Some(f1));
        assert_eq!(timestamp(b"2026-10-03T23:58:10-00:00"), Some(f1));
        let bads: &[&[u8]] = &[
            b"", b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z", b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10", b"1791071890000.0", b"1.79e12", b"-5", b"2026-13-03T23:58:10Z",
            b"2026-02-30T23:58:10Z", b"2027-02-29T23:58:10Z", b"2026-10-03T24:58:10Z",
            b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200", b"2026-10-03T23:58:10.123Z",
        ];
        for &bad in bads {
            assert_eq!(timestamp(bad), None, "{:?}", std::str::from_utf8(bad));
        }
        assert!(timestamp(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(timestamp(b"123456789012345678901"), Some(i64::MAX));
    }

    #[test]
    fn month_table_matches_formula() {
        for y in [1899i64, 1900, 1970, 2000, 2024, 2025, 2028, 2100, 2199, 2200, 9999] {
            for m in 1..=12 {
                for d in 1..=31 {
                    let s = format!("{y:04}-{m:02}-{d:02}T12:34:56Z");
                    let want = (d <= days_in_month(y, m))
                        .then(|| ((days_from_civil(y, m, d) * 24 + 12) * 60 + 34) * 60_000 + 56_000);
                    assert_eq!(timestamp(s.as_bytes()), want, "{s}");
                }
            }
        }
    }

    #[test]
    fn prices() {
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000000"), Some(10_000_000));
        assert_eq!(price(b"10000001"), None);
        assert_eq!(price(b"0"), None);
        assert_eq!(price(b"19.99"), None);
        assert_eq!(price(b"-5"), None);
        assert_eq!(price(b"000000000000000000000001"), Some(1));
    }

    /// The SIMD splitter must agree with the plain one on every line.
    #[test]
    fn simd_matches_scalar() {
        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut rnd = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let alphabet = b"ab\",,\r 1Z\"";
        let mut buf = Vec::new();
        let mut lines = Vec::new();
        for _ in 0..20_000 {
            let start = buf.len();
            let len = rnd(140) as usize;
            for _ in 0..len {
                let r = rnd(100);
                let c = if r < 60 { b'a' + (r % 26) as u8 } else { alphabet[(r % alphabet.len() as u64) as usize] };
                buf.push(c);
            }
            // sometimes build a well-formed 4-field line with random quoting
            if rnd(2) == 0 {
                buf.truncate(start);
                for k in 0..4 {
                    if k > 0 {
                        buf.push(b',');
                    }
                    let q = rnd(3) == 0;
                    if q {
                        buf.push(b'"');
                    }
                    for _ in 0..rnd(12) {
                        buf.push(b'a' + rnd(26) as u8);
                    }
                    if q {
                        buf.push(b'"');
                    }
                }
            }
            lines.push((start, buf.len()));
            buf.push(b'\n');
        }
        buf.extend_from_slice(&[b'x'; 200]);
        let end = buf.len() - 200;
        let mut p = 0;
        for &(s, e) in &lines {
            assert_eq!(p, s);
            let mut a = [&b""[..]; 4];
            let mut b = [&b""[..]; 4];
            let (next, ok) = next_line(&buf, p, end, &mut a);
            let ok2 = split(&buf[s..e], &mut b);
            assert_eq!(ok, ok2, "{:?}", String::from_utf8_lossy(&buf[s..e]));
            if ok {
                assert_eq!(a, b);
            }
            assert_eq!(next, e + 1);
            p = next;
        }
    }

    #[test]
    fn splitting() {
        let mut f = [&b""[..]; 2];
        assert!(split(b"\"u4\",\"u1\"", &mut f) && f[0] == &b"u4"[..] && f[1] == &b"u1"[..]);
        assert!(split(b"u1,", &mut f) && f[1].is_empty());
        assert!(split(b"\"\",u1", &mut f) && f[0].is_empty());
        assert!(!split(b"u1,u5\"", &mut f));
        assert!(!split(b"u1,u2,u3", &mut f));
        assert!(!split(b"", &mut f));
        assert!(!split(b"u1", &mut f));
        assert!(!split(b"\",u1", &mut f));
    }
}


// ---------------------------------------------------------------- SWAR
// Eight ASCII digits in one little-endian u64: validated together and
// converted with three multiplies (Lemire, "SWAR explained: parsing eight
// digits").

#[inline(always)]
fn load8(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

/// True if all eight bytes are ASCII digits.
#[inline(always)]
fn all_digits(v: u64) -> bool {
    ((v & 0xF0F0_F0F0_F0F0_F0F0) | (((v.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) >> 4))
        == 0x3333_3333_3333_3333
}

/// The value of eight digits (byte 0 most significant). `v` must be all digits.
#[inline(always)]
fn value8(v: u64) -> u64 {
    let v = v & 0x0F0F_0F0F_0F0F_0F0F;
    let v = (v.wrapping_mul(2561)) >> 8;
    let v = ((v & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601)) >> 16;
    ((v & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001)) >> 32
}

/// The first k (1..=8) digits of b as one value, or None if not all digits.
#[inline(always)]
fn lead(b: &[u8], k: usize) -> Option<u64> {
    // shift the k bytes to the top and fill the low bytes with '0'
    let v = if b.len() >= 8 {
        load8(b, 0)
    } else {
        let mut a = [b'0'; 8];
        a[..b.len()].copy_from_slice(b);
        u64::from_le_bytes(a)
    };
    let sh = 8 * (8 - k) as u32;
    let v = if k == 8 { v } else { (v << sh) | (0x3030_3030_3030_3030u64 >> (64 - sh)) };
    all_digits(v).then(|| value8(v))
}

/// F3: digits only, leading zeros allowed.
#[inline(always)]
fn epoch_ms(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n <= 8 {
        return lead(b, n).map(|v| v as i64);
    }
    if n <= 16 {
        let lo = load8(b, n - 8);
        if !all_digits(lo) {
            return None;
        }
        let hi = lead(b, n - 8)?;
        return Some((hi * 100_000_000 + value8(lo)) as i64);
    }
    timestamp_ref(b)
}

/// `YYYY-MM-DDTHH:MM:SS` in b[..19] (b.len() >= 20), as epoch ms.
#[inline(always)]
fn datetime(b: &[u8]) -> Option<i64> {
    let a = load8(b, 0); // YYYY-MM-
    let c = load8(b, 8); // DDTHH:MM
    const SA: u64 = (0xFF << 32) | (0xFF << 56);
    const SC: u64 = (0xFF << 16) | (0xFF << 40);
    if a & SA != ((b'-' as u64) << 32 | (b'-' as u64) << 56)
        || c & SC != ((b'T' as u64) << 16 | (b':' as u64) << 40)
        || b[16] != b':'
    {
        return None;
    }
    let a0 = (a & !SA) | (0x30 << 32) | (0x30 << 56);
    let c0 = (c & !SC) | (0x30 << 16) | (0x30 << 40);
    let (s1, s2) = (b[17].wrapping_sub(b'0'), b[18].wrapping_sub(b'0'));
    if !all_digits(a0) || !all_digits(c0) || s1 > 9 || s2 > 9 {
        return None;
    }
    let va = a0 & 0x0F0F_0F0F_0F0F_0F0F;
    let vc = c0 & 0x0F0F_0F0F_0F0F_0F0F;
    let byte = |v: u64, i: u32| ((v >> (8 * i)) & 0xFF) as i64;
    let y = byte(va, 0) * 1000 + byte(va, 1) * 100 + byte(va, 2) * 10 + byte(va, 3);
    let mo = byte(va, 5) * 10 + byte(va, 6);
    let d = byte(vc, 0) * 10 + byte(vc, 1);
    let h = byte(vc, 3) * 10 + byte(vc, 4);
    let mi = byte(vc, 6) * 10 + byte(vc, 7);
    let sec = (s1 * 10 + s2) as i64;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || sec > 59 || d < 1 {
        return None;
    }
    if (TABLE_Y0..TABLE_Y0 + TABLE_YEARS).contains(&y) {
        let (start, dim) = MONTHS[((y - TABLE_Y0) * 12 + mo - 1) as usize];
        if d > dim as i64 {
            return None;
        }
        return Some((((start as i64 + d - 1) * 24 + h) * 60 + mi) * 60_000 + sec * SECOND);
    }
    if d > days_in_month(y, mo) {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + sec * SECOND)
}

/// Any of the three forms, as epoch ms. None for a malformed value; an F3
/// value too big for i64 is also None, which is just as outside every window.
#[inline]
pub fn timestamp(b: &[u8]) -> Option<i64> {
    let len = b.len();
    if len == 0 {
        return None;
    }
    if len == 20 && b[19] == b'Z' {
        return datetime(b);
    }
    if len == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base = datetime(b)?;
        let oh = two(b, 20)?;
        let om = two(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if b[19] == b'+' { base - off } else { base + off });
    }
    if b[0].is_ascii_digit() {
        return epoch_ms(b);
    }
    None
}

#[cfg(test)]
mod swar_tests {
    use super::*;

    /// The SWAR parser agrees with the scalar reference on valid timestamps
    /// of all three forms and on many corrupted variants of them.
    #[test]
    fn swar_matches_reference() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut rnd = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let alphabet = b"0123456789-T:Z+tz .e\r\xd9\x30";
        let mut checked = 0;
        for _ in 0..300_000 {
            let ms = rnd(6_000_000_000_000) as i64 - 1_000_000_000_000;
            let days = ms.div_euclid(DAY);
            let (y, m, d) = civil_from_days(days);
            let sod = ms.rem_euclid(DAY) / 1000;
            let (h, mi, sc) = (sod / 3600, sod / 60 % 60, sod % 60);
            let mut t: Vec<u8> = match rnd(4) {
                0 => format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{sc:02}Z").into_bytes(),
                1 => {
                    let sign = if rnd(2) == 0 { '+' } else { '-' };
                    let (oh, om) = (rnd(26), rnd(62));
                    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{sc:02}{sign}{oh:02}:{om:02}").into_bytes()
                }
                2 => format!("{}{}", "0".repeat(rnd(4) as usize), ms.max(0)).into_bytes(),
                _ => {
                    let e = rnd(18) as u32 + 1;
                    format!("{}", rnd(10u64.pow(e))).into_bytes()
                }
            };
            // corrupt some of them
            match rnd(5) {
                0 if !t.is_empty() => {
                    let i = rnd(t.len() as u64) as usize;
                    t[i] = alphabet[rnd(alphabet.len() as u64) as usize];
                }
                1 if !t.is_empty() => {
                    t.truncate(rnd(t.len() as u64) as usize);
                }
                2 => t.push(alphabet[rnd(alphabet.len() as u64) as usize]),
                _ => {}
            }
            assert_eq!(timestamp(&t), timestamp_ref(&t), "{:?}", String::from_utf8_lossy(&t));
            checked += 1;
        }
        // month/day edge cases around leap years
        for y in [1900i64, 2000, 2024, 2027, 2028, 2100] {
            for m in 1..=13 {
                for d in 0..=32 {
                    let t = format!("{y:04}-{m:02}-{d:02}T23:59:59Z");
                    assert_eq!(timestamp(t.as_bytes()), timestamp_ref(t.as_bytes()), "{t}");
                }
            }
        }
        assert!(checked > 0);
    }
}


// ---------------------------------------------------------------- block scanning
/// Comma, newline and quote bitmasks of the 64 bytes at p.
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let lo = _mm256_loadu_si256(p as *const __m256i);
    let hi = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |v: __m256i, c: u8| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c as i8))) as u32 as u64;
    (
        m(lo, b',') | (m(hi, b',') << 32),
        m(lo, b'\n') | (m(hi, b'\n') << 32),
        m(lo, b'"') | (m(hi, b'"') << 32),
    )
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let b = *p.add(i);
        c |= ((b == b',') as u64) << i;
        n |= ((b == b'\n') as u64) << i;
        q |= ((b == b'"') as u64) << i;
    }
    (c, n, q)
}

/// Calls f for every well-formed row among the lines in data[start..end].
/// Scans 64-byte blocks once and walks separator bits across lines. A line
/// is well-formed when it has exactly N-1 commas and its quote count equals
/// twice the number of fields that start and end with a quote (and are at
/// least two bytes long): any other quote breaks the equality.
#[inline(always)]
pub fn for_rows<'a, const N: usize>(data: &'a [u8], start: usize, end: usize, f: impl FnMut(&[&'a [u8]; N])) {
    #[cfg(target_arch = "x86_64")]
    {
        if use_avx512() {
            // SAFETY: the CPU supports AVX-512F/BW (checked at run time).
            return unsafe { for_rows_avx512::<N>(data, start, end, f) };
        }
    }
    for_rows_impl::<N, false>(data, start, end, f)
}

/// AVX-512 is used when the CPU has it, unless SESSIONS_NO_AVX512 is set.
#[cfg(target_arch = "x86_64")]
#[inline]
fn use_avx512() -> bool {
    static FLAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FLAG.get_or_init(|| {
        let on = std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::env::var_os("SESSIONS_NO_AVX512").is_none();
        eprintln!("splitter: {}", if on { "avx512" } else { "avx2" });
        on
    })
}

/// The block scanner compiled with AVX-512 enabled (one 64-byte compare per mask).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn for_rows_avx512<'a, const N: usize>(data: &'a [u8], start: usize, end: usize, f: impl FnMut(&[&'a [u8]; N])) {
    for_rows_impl::<N, true>(data, start, end, f)
}

/// Comma, newline and quote bitmasks of the 64 bytes at p, with AVX-512.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
#[inline]
unsafe fn masks64_512(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let v = _mm512_loadu_si512(p as *const _);
    (
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
    )
}

#[inline(always)]
fn masks64_any<const W: bool>(p: *const u8) -> (u64, u64, u64) {
    #[cfg(target_arch = "x86_64")]
    {
        if W {
            // SAFETY: W is only true inside for_rows_avx512.
            return unsafe { masks64_512(p) };
        }
    }
    unsafe { masks64(p) }
}

#[inline(always)]
fn for_rows_impl<'a, const N: usize, const W: bool>(data: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; N])) {
    let mut out: [&'a [u8]; N] = [&data[..0]; N];
    let mut commas = [0usize; N];
    let mut nc = 0usize; // commas seen in the current line (capped at N)
    let mut nq = 0u32; // quotes seen in the current line
    let mut line = start;
    let mut emit = |line: usize, nl: usize, commas: &[usize; N], nc: usize, nq: u32, out: &mut [&'a [u8]; N]| {
        if nc != N - 1 {
            return;
        }
        let mut quoted = 0u32;
        let mut a = line;
        for i in 0..N {
            let b = if i + 1 < N { commas[i] } else { nl };
            let (mut fa, mut fb) = (a, b);
            if nq != 0 && b > a && data[a] == b'"' {
                if b - a < 2 || data[b - 1] != b'"' {
                    return;
                }
                quoted += 1;
                fa += 1;
                fb -= 1;
            }
            out[i] = &data[fa..fb];
            a = b + 1;
        }
        if nq == 2 * quoted {
            f(out);
        }
    };
    let mut base = start;
    let mut buf: [u8; 64];
    while base < end {
        let (mut cm, mut nm, mut qm) = if base + 64 <= data.len() {
            masks64_any::<W>(unsafe { data.as_ptr().add(base) })
        } else {
            let n = data.len() - base;
            buf = [0u8; 64];
            buf[..n].copy_from_slice(&data[base..]);
            masks64_any::<W>(buf.as_ptr())
        };
        if end - base < 64 {
            let keep = (1u64 << (end - base)) - 1;
            cm &= keep;
            nm &= keep;
            qm &= keep;
        }
        let mut sep = cm | nm;
        while sep != 0 {
            let bit = sep.trailing_zeros();
            let pos = base + bit as usize;
            if (nm >> bit) & 1 != 0 {
                // quotes up to this newline belong to the line
                let upto = if bit == 63 { u64::MAX } else { (1u64 << (bit + 1)) - 1 };
                nq += (qm & upto).count_ones();
                qm &= !upto;
                emit(line, pos, &commas, nc, nq, &mut out);
                line = pos + 1;
                nc = 0;
                nq = 0;
            } else {
                if nc < N - 1 {
                    commas[nc] = pos;
                }
                nc = (nc + 1).min(N);
            }
            sep &= sep - 1;
        }
        nq += qm.count_ones();
        base += 64;
    }
    if line < end {
        emit(line, end, &commas, nc, nq, &mut out);
    }
}

#[cfg(test)]
mod block_tests {
    use super::*;

    fn rows<const N: usize>(data: &[u8], start: usize, end: usize, block: bool) -> Vec<Vec<Vec<u8>>> {
        let mut v = Vec::new();
        let mut push = |r: &[&[u8]; N]| v.push(r.iter().map(|x| x.to_vec()).collect());
        if block {
            for_rows::<N>(data, start, end, &mut push);
        } else {
            for_rows_ref::<N>(data, start, end, &mut push);
        }
        v
    }

    /// The block scanner yields exactly the rows of the per-line splitter.
    #[test]
    fn block_matches_reference() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut rnd = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let alphabet = b"ab\",,\r 1Z\"\n";
        for round in 0..400 {
            let mut buf = Vec::new();
            let lines = rnd(300) as usize;
            for _ in 0..lines {
                if rnd(3) == 0 {
                    // random garbage line
                    for _ in 0..rnd(150) {
                        let r = rnd(100);
                        buf.push(if r < 55 { b'a' + (r % 26) as u8 } else { alphabet[(r % alphabet.len() as u64) as usize] });
                    }
                } else {
                    let nf = if rnd(8) == 0 { rnd(9) as usize } else { 4 };
                    for k in 0..nf {
                        if k > 0 {
                            buf.push(b',');
                        }
                        let q = rnd(4) == 0;
                        if q {
                            buf.push(b'"');
                        }
                        for _ in 0..rnd(40) {
                            buf.push(b'a' + rnd(26) as u8);
                        }
                        if rnd(30) == 0 {
                            buf.push(b'"');
                        }
                        if q && rnd(10) != 0 {
                            buf.push(b'"');
                        }
                    }
                }
                buf.push(b'\n');
            }
            if rnd(2) == 0 {
                buf.pop(); // last line without a newline
            }
            // sometimes end exactly at the data end, sometimes leave padding
            if round % 2 == 0 {
                buf.extend_from_slice(&[b'x'; 70]);
                let end = buf.len() - 70;
                assert_eq!(rows::<4>(&buf, 0, end, true), rows::<4>(&buf, 0, end, false));
                assert_eq!(rows::<2>(&buf, 0, end, true), rows::<2>(&buf, 0, end, false));
            } else {
                let end = buf.len();
                assert_eq!(rows::<4>(&buf, 0, end, true), rows::<4>(&buf, 0, end, false));
                assert_eq!(rows::<7>(&buf, 0, end, true), rows::<7>(&buf, 0, end, false));
            }
        }
    }
}
