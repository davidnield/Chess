//! A small, fast hasher for keys whose first field is already a Zobrist hash.
//! SipHash (std's default) is several times slower and buys nothing here: no
//! key is attacker-controlled.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

#[derive(Default, Clone, Copy)]
pub struct Mix(u64);

impl Hasher for Mix {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut b = [0u8; 8];
            b[..chunk.len()].copy_from_slice(chunk);
            self.write_u64(u64::from_le_bytes(b));
        }
    }

    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = (self.0 ^ v).wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(29);
    }

    #[inline]
    fn write_u32(&mut self, v: u32) {
        self.write_u64(u64::from(v));
    }

    #[inline]
    fn write_u8(&mut self, v: u8) {
        self.write_u64(u64::from(v));
    }

    #[inline]
    fn finish(&self) -> u64 {
        // splitmix64's finalizer: every output bit depends on every input bit,
        // which hashbrown needs (it uses the top 7 bits AND the low bits).
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

pub type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<Mix>>;
pub type FastSet<K> = HashSet<K, BuildHasherDefault<Mix>>;
