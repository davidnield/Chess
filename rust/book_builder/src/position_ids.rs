//! Run-local IDs for kernels that take i64 columns. Every external join uses (lo, hi).
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(feature = "python")]
use numpy::{PyArray1, PyReadonlyArray1};
#[cfg(feature = "python")]
use pyo3::exceptions::PyValueError;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[derive(Default)]
pub struct PositionIds {
    // Common case: the low half itself is available as the ID.
    high: HashMap<i64, i64>,
    aliases: HashMap<(i64, i64), i64>,
    reverse: HashMap<i64, (i64, i64)>,
    next: u64,
}

impl PositionIds {
    pub fn intern(&mut self, lo: i64, hi: i64) -> i64 {
        if self.high.get(&lo) == Some(&hi) {
            return lo;
        }
        if !self.high.contains_key(&lo) && !self.reverse.contains_key(&lo) {
            self.high.insert(lo, hi);
            return lo;
        }
        if let Some(&id) = self.aliases.get(&(lo, hi)) {
            return id;
        }
        loop {
            let id = (self.next ^ (1 << 63)) as i64;
            self.next = self.next.checked_add(1).expect("position ID space exhausted");
            if !self.high.contains_key(&id) && !self.reverse.contains_key(&id) {
                self.aliases.insert((lo, hi), id);
                self.reverse.insert(id, (lo, hi));
                return id;
            }
        }
    }

    pub fn ordering_exceptions(&self) -> HashMap<i64, (i64, i64)> {
        let mut pairs = self.reverse.clone();
        for &(lo, _) in self.reverse.values() {
            if let Some(&hi) = self.high.get(&lo) {
                pairs.insert(lo, (lo, hi));
            }
        }
        pairs
    }

    pub fn pair(&self, id: i64) -> Option<(i64, i64)> {
        self.high.get(&id).map(|&hi| (id, hi)).or_else(|| self.reverse.get(&id).copied())
    }
}

pub type SharedIds = Arc<Mutex<PositionIds>>;

#[cfg(feature = "python")]
#[pyclass(module = "book_builder_rs")]
pub struct RsPositionIds {
    pub inner: SharedIds,
}

#[cfg(feature = "python")]
#[pymethods]
impl RsPositionIds {
    #[new]
    fn new() -> Self { Self { inner: SharedIds::default() } }

    fn encode<'py>(&self, py: Python<'py>, lo: PyReadonlyArray1<i64>, hi: PyReadonlyArray1<i64>)
                   -> PyResult<Bound<'py, PyArray1<i64>>> {
        let (lo, hi) = (lo.as_slice()?, hi.as_slice()?);
        crate::same_len(lo.len(), &[hi.len()])?;
        let mut keys = self.inner.lock().unwrap();
        Ok(PyArray1::from_vec(py, lo.iter().zip(hi).map(|(&l,&h)| keys.intern(l,h)).collect()))
    }

    fn encode_one(&self, lo: i64, hi: i64) -> i64 {
        self.inner.lock().unwrap().intern(lo, hi)
    }

    fn encode_epds<'py>(&self, py: Python<'py>, lows: PyReadonlyArray1<i64>, epds: Vec<String>)
                       -> PyResult<Bound<'py, PyArray1<i64>>> {
        use shakmaty::{EnPassantMode, Position, zobrist::Zobrist128};
        let lows = lows.as_slice()?;
        crate::same_len(lows.len(), &[epds.len()])?;
        let mut keys = self.inner.lock().unwrap();
        let mut out = Vec::with_capacity(lows.len());
        for (&lo, epd) in lows.iter().zip(epds) {
            let pos = crate::rollout::from_epd(&epd).map_err(PyValueError::new_err)?;
            let mut z: Zobrist128 = pos.zobrist_hash(EnPassantMode::PseudoLegal);
            // Legacy EPD writers sometimes dropped a pseudo-legal EP square because
            // the capturing pawn was pinned. The recorded low disambiguates its file.
            if z.0 as i64 != lo && epd.split_whitespace().nth(3) == Some("-") {
                let prefix = epd.split_whitespace().take(3).collect::<Vec<_>>().join(" ");
                let rank = if pos.turn() == shakmaty::Color::White { '6' } else { '3' };
                for file in 'a'..='h' {
                    if let Ok(candidate) = crate::rollout::from_epd(&format!("{prefix} {file}{rank}")) {
                        if candidate.legal_ep_square().is_some() { continue; }
                        let maybe: Zobrist128 = candidate.zobrist_hash(EnPassantMode::PseudoLegal);
                        if maybe.0 as i64 == lo { z = maybe; break; }
                    }
                }
            }
            if z.0 as i64 != lo {
                return Err(PyValueError::new_err(format!("legacy hash disagrees with board: {epd}")));
            }
            out.push(keys.intern(lo, (z.0 >> 64) as i64));
        }
        Ok(PyArray1::from_vec(py, out))
    }

    fn decode<'py>(&self, py: Python<'py>, ids: PyReadonlyArray1<i64>)
                   -> PyResult<(Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>)> {
        let keys = self.inner.lock().unwrap();
        let ids = ids.as_slice()?;
        let (mut lo, mut hi) = (Vec::with_capacity(ids.len()), Vec::with_capacity(ids.len()));
        for &id in ids {
            let (l, h) = keys.pair(id).ok_or_else(|| PyValueError::new_err("unknown position ID"))?;
            lo.push(l);
            hi.push(h);
        }
        Ok((PyArray1::from_vec(py, lo), PyArray1::from_vec(py, hi)))
    }
}

#[test]
fn alias_can_itself_be_a_later_low_half() {
    let mut keys = PositionIds::default();
    let a = keys.intern(-17, 1);
    let b = keys.intern(-17, 2);
    let c = keys.intern(b, 2);
    assert!(a != b && b != c && a != c);
    for (id, pair) in [(a,(-17,1)), (b,(-17,2)), (c,(b,2))] {
        assert_eq!(keys.pair(id), Some(pair));
        assert_eq!(keys.intern(pair.0, pair.1), id);
    }
}
