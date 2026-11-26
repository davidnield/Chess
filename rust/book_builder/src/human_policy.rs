//! Versioned human move policies and probability-preserving value arithmetic.
//! No support threshold switches the weak player's observations to adult counts.

fn vector(v: &[f64], n: usize, probability: bool) -> Result<(), String> {
    if n == 0 || v.len() != n || v.iter().any(|x| !x.is_finite() || *x < 0.) {
        return Err("invalid policy vector".into());
    }
    if probability && (v.iter().sum::<f64>() - 1.).abs() > 1e-10 {
        return Err("policy probabilities do not sum to one".into());
    }
    Ok(())
}

/// `cap` (own-policy observation cap, David 2026-11-26): at most `cap` observations count, so at
/// popular positions, where many weak players are still in their own book, the model keeps a share.
/// Counts are scaled proportionally, preserving their move shares. `None` is the uncapped v1 formula.
pub fn own(counts: &[f64], maia: &[f64], strength: f64, cap: Option<f64>) -> Result<Vec<f64>, String> {
    vector(counts, counts.len(), false)?;
    vector(maia, counts.len(), true)?;
    if !strength.is_finite() || strength <= 0. { return Err("invalid own strength".into()); }
    if cap.is_some_and(|c| !c.is_finite() || c <= 0.) { return Err("invalid own observation cap".into()); }
    let n = counts.iter().sum::<f64>();
    if !n.is_finite() { return Err("nonfinite count total".into()); }
    let scale = match cap { Some(c) if n > c => c / n, _ => 1. };
    let ne = n * scale;
    let result: Vec<_> = counts.iter().zip(maia).map(|(c,q)| (c*scale+strength*q)/(ne+strength)).collect();
    vector(&result, counts.len(), true)?;
    Ok(result)
}

#[derive(Clone, Copy)]
pub struct Opponent {
    pub otb: f64, pub empirical: f64, pub maia: f64,
    pub backup_discount: f64, pub broad_discount: f64, pub gate: Option<f64>,
    /// otb-ensemble-v3 (David 2026-11-26): an explicit population mix, reply =
    /// mix*OTB-component + (1-mix)*online, instead of OTB counts overriding the online model as they
    /// grow. `None` is v2: reply = OTB-component.
    pub mix: Option<f64>,
}

pub fn opponent(b: &[f64], a: &[f64], r: &[f64], o: &[f64], q: Option<&[f64]>,
                cfg: Opponent) -> Result<Vec<f64>, String> {
    let l=b.len();
    for v in [b,a,r,o] { vector(v,l,false)?; }
    for i in 0..l { if b[i]<a[i] || a[i]<r[i] { return Err("overlapping/superset counts do not support subtraction".into()); } }
    if [cfg.otb,cfg.empirical,cfg.maia].iter().any(|x| !x.is_finite() || *x<=0.) ||
       [cfg.backup_discount,cfg.broad_discount].iter().any(|x| !x.is_finite() || !(0. ..=1.).contains(x)) ||
       cfg.gate.is_some_and(|g| !g.is_finite() || g<=0.) ||
       cfg.mix.is_some_and(|m| !m.is_finite() || !(0. ..=1.).contains(&m)) { return Err("invalid opponent parameters".into()); }
    let (bt,at,rt,ot)=(b.iter().sum::<f64>(),a.iter().sum::<f64>(),r.iter().sum::<f64>(),o.iter().sum::<f64>());
    if [bt,at,rt,ot].iter().any(|x| !x.is_finite()) { return Err("nonfinite count total".into()); }
    let s=rt+cfg.backup_discount*(at-rt)+cfg.broad_discount*(bt-at);
    let use_maia=(bt==0. && ot==0.) || cfg.gate.is_none_or(|g| s<g);
    let q=if use_maia {
        let q=q.ok_or("this board's support is below the gate: Maia is required")?;
        vector(q,l,true)?; q
    } else { &[] };
    if bt==0. && ot==0. { return Ok(q.to_vec()); }
    let k=cfg.empirical;
    let result:Vec<_>=(0..l).map(|i| {
        let broad=(b[i]+1./l as f64)/(bt+1.);
        let backup=(a[i]-r[i]+k*broad)/(at-rt+k);
        let online=(r[i]+k*backup)/(rt+k);
        let pre=if use_maia { (s*online+cfg.maia*q[i])/(s+cfg.maia) } else { online };
        let otb=(o[i]+cfg.otb*pre)/(ot+cfg.otb);
        match cfg.mix { Some(m) => m*otb+(1.-m)*pre, None => otb }
    }).collect();
    vector(&result,l,true)?;
    Ok(result)
}

/// A missing child keeps its full [0,1] interval; no renormalization or implicit .5.
pub fn expectation(p: &[f64], lower: &[f64], upper: &[f64]) -> Result<(f64,f64),String> {
    vector(p,p.len(),true)?;
    if lower.len()!=p.len() || upper.len()!=p.len() { return Err("value lengths differ".into()); }
    let (mut lo,mut hi)=(0.,0.);
    for i in 0..p.len() {
        if !lower[i].is_finite() || !upper[i].is_finite() || lower[i]<0. || upper[i]>1. || lower[i]>upper[i] {
            return Err("invalid continuation interval".into());
        }
        lo+=p[i]*lower[i]; hi+=p[i]*upper[i];
    }
    Ok((lo,hi))
}

pub fn utility(cp: f64, white: bool) -> f64 {
    let c=cp.clamp(-2000.,2000.) * if white {1.} else {-1.};
    crate::value::cp_to_expected_score(c)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn weak_support_is_continuous() {
        for n in [0.,1.,19.,20.,21.] {
            let p=own(&[n,0.],&[0.8,0.2],30.,None).unwrap();
            assert_eq!(p[0],(n+24.)/(n+30.));
            assert!((p.iter().sum::<f64>()-1.).abs()<1e-15);
        }
        assert!(own(&[1.,0.],&[0.5,0.1],30.,None).is_err());
    }
    #[test] fn own_cap_keeps_shares_and_model_weight() {
        // below the cap: identical to the uncapped formula, bit for bit
        let a=own(&[60.,40.],&[0.3,0.7],30.,Some(100.)).unwrap();
        assert_eq!(a,own(&[60.,40.],&[0.3,0.7],30.,None).unwrap());
        // above it: 6,000/4,000 counts behave as 60/40 of 100 observations
        let b=own(&[6000.,4000.],&[0.3,0.7],30.,Some(100.)).unwrap();
        assert!((b[0]-(60.+9.)/130.).abs()<1e-15);
        assert!((b.iter().sum::<f64>()-1.).abs()<1e-15);
        assert!(own(&[1.,0.],&[0.5,0.5],30.,Some(0.)).is_err());
    }
    fn cfg(mix: Option<f64>) -> Opponent {
        Opponent{otb:10.,empirical:30.,maia:3.,backup_discount:0.1,broad_discount:0.01,gate:Some(300.),mix}
    }
    #[test] fn mix_none_is_v2_and_mix_weights_otb() {
        let (b,a,r)=([9000.,1000.],[5000.,500.],[2000.,200.]);
        let o=[100.,900.];
        let v2=opponent(&b,&a,&r,&o,None,cfg(None)).unwrap();
        let online=opponent(&b,&a,&r,&[0.,0.],None,cfg(None)).unwrap();
        let v3=opponent(&b,&a,&r,&o,None,cfg(Some(0.5))).unwrap();
        for i in 0..2 { assert!((v3[i]-(0.5*v2[i]+0.5*online[i])).abs()<1e-15); }
        // no OTB data: the mix changes nothing
        assert_eq!(opponent(&b,&a,&r,&[0.,0.],None,cfg(Some(0.5))).unwrap(),online);
        assert_eq!(opponent(&b,&a,&r,&o,None,cfg(Some(1.))).unwrap(),v2);
        assert!(opponent(&b,&a,&r,&o,None,cfg(Some(1.5))).is_err());
    }
    #[test] fn missing_mass_and_small_replies_survive() {
        assert_eq!(expectation(&[0.991,0.009],&[1.,0.],&[1.,0.]).unwrap(),(0.991,0.991));
        assert_eq!(expectation(&[0.5,0.5],&[0.8,0.],&[0.8,1.]).unwrap(),(0.4,0.9));
        assert!(expectation(&[0.5],&[0.8],&[0.8]).is_err());
    }
    #[test] fn utility_keeps_source_range() {
        assert!(utility(2000.,true)>0.999);
        assert_eq!(utility(3000.,true),utility(2000.,true));
        assert!((utility(12.75,true)+utility(12.75,false)-1.).abs()<1e-15);
    }
}
