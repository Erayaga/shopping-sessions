//! Per-thread bump arenas and write-combined segmented vectors.
//!
//! Partitioning writes every record into one of hundreds of streams per
//! thread. Writing records straight to memory costs a cache miss (a read for
//! ownership of a line nobody will read soon) every line or two, and with
//! hundreds of streams the hardware prefetcher can't help. So each stream
//! stages a few cache lines in a small buffer that stays in L1/L2, and flushes
//! it with non-temporal stores, which write whole lines without reading them.
//! Blocks are carved in order from large per-thread regions (marked for huge
//! pages where the kernel allows), so nothing is ever reallocated or copied.

use memmap2::MmapMut;

const REGION: usize = 64 << 20;
const HUGE: usize = 2 << 20;
pub const BLOCK: usize = 64 << 10;
const STAGE: usize = 320; // bytes: a whole number of 64-byte lines for 16-, 24-, 32- and 40-byte records

pub struct Arena {
    maps: Vec<MmapMut>,
    cur: usize,
    end: usize,
}

// The arena only hands out raw blocks; ownership moves with it between threads.
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Default for Arena {
    fn default() -> Self {
        Arena { maps: Vec::new(), cur: 0, end: 0 }
    }
}

impl Arena {
    #[inline]
    fn block(&mut self) -> *mut u8 {
        if self.cur + BLOCK > self.end {
            let mut m = MmapMut::map_anon(REGION + HUGE).expect("map anonymous region");
            #[cfg(target_os = "linux")]
            {
                let _ = m.advise(memmap2::Advice::HugePage);
            }
            let base = m.as_mut_ptr() as usize;
            self.cur = (base + HUGE - 1) & !(HUGE - 1);
            self.end = self.cur + REGION;
            self.maps.push(m);
        }
        let p = self.cur as *mut u8;
        self.cur += BLOCK;
        p
    }
}

const fn gcd(a: usize, b: usize) -> usize {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

#[repr(C, align(64))]
struct Stage([u8; STAGE]);

/// A push-only vector made of BLOCK-sized blocks from an Arena, written
/// through a staging buffer. Call `finish` before reading.
pub struct Seg<T: Copy> {
    stage: Stage,
    nstage: usize,
    blocks: Vec<*mut T>,
    last: usize, // records in the last block
}

unsafe impl<T: Copy + Send> Send for Seg<T> {}
unsafe impl<T: Copy + Sync> Sync for Seg<T> {}

impl<T: Copy> Default for Seg<T> {
    fn default() -> Self {
        Seg { stage: Stage([0; STAGE]), nstage: 0, blocks: Vec::new(), last: Self::CAP }
    }
}

/// Copies `len` bytes (a multiple of 64) from src to 64-byte-aligned dst with
/// non-temporal stores.
#[inline(always)]
unsafe fn stream_copy(dst: *mut u8, src: *const u8, len: usize) {
    #[cfg(target_feature = "avx2")]
    {
        use std::arch::x86_64::*;
        let mut i = 0;
        while i < len {
            let v = _mm256_load_si256(src.add(i) as *const __m256i);
            _mm256_stream_si256(dst.add(i) as *mut __m256i, v);
            i += 32;
        }
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        std::ptr::copy_nonoverlapping(src, dst, len);
    }
}

impl<T: Copy> Seg<T> {
    const SIZE: usize = std::mem::size_of::<T>();
    /// Records per flush: the smallest count filling whole cache lines.
    const K: usize = 64 / gcd(Self::SIZE, 64);
    /// Records per block, a multiple of K so flushes stay line-aligned.
    const CAP: usize = (BLOCK / Self::SIZE) / Self::K * Self::K;
    const OK: () = assert!(Self::K * Self::SIZE <= STAGE && (Self::K * Self::SIZE) % 64 == 0);

    #[inline(always)]
    pub fn push(&mut self, arena: &mut Arena, v: T) {
        #[allow(clippy::let_unit_value)]
        let () = Self::OK;
        unsafe { (self.stage.0.as_mut_ptr() as *mut T).add(self.nstage).write_unaligned(v) };
        self.nstage += 1;
        if self.nstage == Self::K {
            self.flush(arena);
        }
    }

    #[inline(never)]
    fn flush(&mut self, arena: &mut Arena) {
        if self.last == Self::CAP {
            self.blocks.push(arena.block() as *mut T);
            self.last = 0;
        }
        unsafe {
            let dst = self.blocks.last().unwrap_unchecked().add(self.last) as *mut u8;
            stream_copy(dst, self.stage.0.as_ptr(), Self::K * Self::SIZE);
        }
        self.last += Self::K;
        self.nstage = 0;
    }

    /// Moves the staged tail into the blocks (plain stores).
    pub fn finish(&mut self, arena: &mut Arena) {
        if self.nstage == 0 {
            return;
        }
        if self.last == Self::CAP {
            self.blocks.push(arena.block() as *mut T);
            self.last = 0;
        }
        unsafe {
            let dst = self.blocks.last().unwrap_unchecked().add(self.last) as *mut u8;
            std::ptr::copy_nonoverlapping(self.stage.0.as_ptr(), dst, self.nstage * Self::SIZE);
        }
        self.last += self.nstage;
        self.nstage = 0;
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        if self.blocks.is_empty() {
            0
        } else {
            (self.blocks.len() - 1) * Self::CAP + self.last
        }
    }

    #[inline]
    pub fn slices(&self) -> impl Iterator<Item = &[T]> {
        let n = self.blocks.len();
        self.blocks.iter().enumerate().map(move |(i, &p)| {
            let len = if i + 1 == n { self.last } else { Self::CAP };
            unsafe { std::slice::from_raw_parts(p, len) }
        })
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.slices().flat_map(|s| s.iter())
    }
}

/// One thread's output of a partitioning stage: NB streams in one arena.
pub struct Parts<T: Copy> {
    pub arena: Arena,
    pub segs: Vec<Seg<T>>,
}

impl<T: Copy> Parts<T> {
    pub fn new(nb: usize) -> Self {
        Parts { arena: Arena::default(), segs: (0..nb).map(|_| Seg::default()).collect() }
    }
    #[inline(always)]
    pub fn push(&mut self, b: usize, v: T) {
        self.segs[b].push(&mut self.arena, v);
    }
    /// Flushes every stream's staged tail and fences the streaming stores, so
    /// other threads can read the data once this thread is joined.
    pub fn finish(&mut self) {
        for s in &mut self.segs {
            s.finish(&mut self.arena);
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
    }
}

/// All of bucket b across the threads' outputs.
pub fn gather<T: Copy>(parts: &[Parts<T>], b: usize) -> impl Iterator<Item = &T> {
    parts.iter().flat_map(move |p| p.segs[b].iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seg_roundtrip() {
        fn check<T: Copy + PartialEq + std::fmt::Debug>(mk: impl Fn(u64) -> T) {
            let mut p: Parts<T> = Parts::new(3);
            for i in 0..100_003u64 {
                p.push((i % 3) as usize, mk(i));
            }
            p.finish();
            let parts = vec![p];
            for b in 0..3 {
                let v: Vec<T> = gather(&parts, b).copied().collect();
                let want: Vec<T> = (0..100_003u64).filter(|i| i % 3 == b as u64).map(&mk).collect();
                assert_eq!(v, want);
            }
        }
        check(|i| (i, i as u32 * 7)); // 16 bytes
        check(|i| (i, i ^ 5, i as u32)); // 24 bytes
        check(|i| (i, i ^ 5, i * 3, i as u32, i as u32 + 1)); // 32 bytes
        check(|i| (i, i ^ 5, i * 3, i as u32, i as u32 + 1, i as u32 + 2)); // 40 bytes
    }
}
