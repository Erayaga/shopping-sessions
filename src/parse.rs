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
fn datetime(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = two(b, 0)? * 100 + two(b, 2)?;
    let mo = two(b, 5)?;
    let d = two(b, 8)?;
    let h = two(b, 11)?;
    let mi = two(b, 14)?;
    let s = two(b, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * SECOND)
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
    fn prices() {
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000000"), Some(10_000_000));
        assert_eq!(price(b"10000001"), None);
        assert_eq!(price(b"0"), None);
        assert_eq!(price(b"19.99"), None);
        assert_eq!(price(b"-5"), None);
        assert_eq!(price(b"000000000000000000000001"), Some(1));
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
