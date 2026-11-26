//! Streaming publication checks and raw-prediction transforms. No corpus retained.
use numpy::{PyReadonlyArray1, PyReadonlyArray2};
use pyo3::{exceptions::PyValueError, prelude::*};
use sha2::{Digest, Sha256};

fn fail(s: &str) -> PyErr { PyValueError::new_err(s.to_owned()) }

#[pyfunction]
pub fn prediction_temperature(raw: Vec<f64>, temperature: f64) -> PyResult<Vec<f64>> {
    let sum: f64 = raw.iter().sum();
    if raw.is_empty() || raw.iter().any(|v| !v.is_finite() || *v < 0.)
        || !sum.is_finite() || (sum - 1.).abs() > 1e-5
        || !temperature.is_finite() || temperature <= 0. {
        return Err(fail("invalid raw prediction or temperature"));
    }
    let logs: Vec<_> = raw.iter().map(|p| p.ln()).collect();
    let largest = logs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let values: Vec<_> = logs.iter().map(|v| ((v-largest)/temperature).exp()).collect();
    let total: f64 = values.iter().sum();
    Ok(values.into_iter().map(|v| v/total).collect())
}

#[pyclass]
pub struct RsPoolAudit {
    bucket: i64, cap: i64, aux: bool, previous: Option<(i64,i64,String)>,
    previous_epd: String, rows: u64, parents: u64, sums: Vec<u64>, keys: Sha256,
}

#[pymethods]
impl RsPoolAudit {
    #[new]
    fn new(bucket: i64, cap: i64, aux: bool) -> PyResult<Self> {
        if !(0..512).contains(&bucket) || cap < 1 { return Err(fail("invalid pool audit recipe")); }
        Ok(Self {bucket,cap,aux,previous:None,previous_epd:String::new(),rows:0,parents:0,
            sums:vec![0; if aux {20} else {4}],keys:Sha256::new()})
    }
    fn batch(&mut self, lo: PyReadonlyArray1<'_,i64>, hi: PyReadonlyArray1<'_,i64>,
        sans: Vec<String>, epds: Vec<String>, ply: PyReadonlyArray1<'_,i64>,
        counts: PyReadonlyArray2<'_,i64>) -> PyResult<()> {
        let (lo,hi,ply,counts)=(lo.as_slice()?,hi.as_slice()?,ply.as_slice()?,counts.as_array());
        let n=lo.len();
        if hi.len()!=n || ply.len()!=n || sans.len()!=n || epds.len()!=n
            || counts.nrows()!=n || counts.ncols()!=self.sums.len() {return Err(fail("audit batch shapes differ"));}
        for i in 0..n {
            let key=(lo[i],hi[i],sans[i].clone());
            if lo[i].rem_euclid(512)!=self.bucket || self.previous.as_ref().is_some_and(|p| *p>=key) {
                return Err(fail("wrong bucket, unsorted rows or duplicate full key/move"));
            }
            let new_parent=self.previous.as_ref().is_none_or(|p|(p.0,p.1)!=(lo[i],hi[i]));
            if !self.aux {
                if sans[i].is_empty() || epds[i].is_empty() || ply[i]<1 || ply[i]>self.cap
                    || counts[[i,3]]<1 || (!new_parent && self.previous_epd!=epds[i]) {
                    return Err(fail("invalid edge position, depth or observations"));
                }
            } else if !sans[i].is_empty() || !epds[i].is_empty() {return Err(fail("aux input layout differs"));}
            for j in 0..counts.ncols() {
                let v=counts[[i,j]];
                if v<0 {return Err(fail("negative pool count"));}
                self.sums[j]=self.sums[j].checked_add(v as u64).ok_or_else(||fail("count overflow"))?;
            }
            // Both layouts put white/draw/black/total in groups of four.
            for j in (0..counts.ncols()).step_by(4) {
                let wdl=(counts[[i,j]] as i128)+(counts[[i,j+1]] as i128)+(counts[[i,j+2]] as i128);
                if wdl!=counts[[i,j+3]] as i128 {return Err(fail("pool WDL mass differs"));}
            }
            if self.aux && (16..20).any(|j|counts[[i,j]]!=0) {return Err(fail("uncut pool has Other mass"));}
            if new_parent {
                self.parents+=1;
                self.keys.update(lo[i].to_le_bytes());self.keys.update(hi[i].to_le_bytes());
            }
            self.rows+=1;self.previous=Some(key);self.previous_epd=epds[i].clone();
        }
        Ok(())
    }
    fn result(&self) -> (u64,u64,Vec<u64>,String) {
        (self.rows,self.parents,self.sums.clone(),format!("{:x}",self.keys.clone().finalize()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn temperature_is_stable_and_preserves_zero() {
        let v=prediction_temperature(vec![0.,0.25,0.75],1.).unwrap();
        assert_eq!(v[0],0.);
        assert!((v[1]-0.25).abs()<1e-15 && (v[2]-0.75).abs()<1e-15);
        let v=prediction_temperature(vec![1e-300,1.],0.01).unwrap();
        assert_eq!(v,vec![0.,1.]);
        assert!(prediction_temperature(vec![0.,0.],1.).is_err());
        assert!(prediction_temperature(vec![0.3,0.3],1.).is_err());
        assert!(prediction_temperature(vec![1.],f64::NAN).is_err());
    }
}
