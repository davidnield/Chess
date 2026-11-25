//! Native resumable calibration runner. Completed cells are immutable and rechecked.
use super::*;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema: u32,
    pub budget: usize,
    pub caps: Vec<usize>,
    pub oracle_max_own: usize,
    pub max_input_bytes: u64,
    pub limits: exact::Limits,
    pub neighborhood: search::NeighborhoodConfig,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            schema: 1,
            budget: 5,
            caps: vec![256, 512, 1024],
            oracle_max_own: 12,
            max_input_bytes: 64 * 1024 * 1024,
            limits: exact::Limits::default(),
            neighborhood: search::NeighborhoodConfig::default(),
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.schema != 1
            || self.budget == 0
            || self.budget > 100_000
            || self.caps.is_empty()
            || self.caps.iter().any(|&c| c == 0)
            || self.max_input_bytes == 0
            || self.oracle_max_own > 24
        {
            return Err("invalid benchmark configuration".into());
        }
        if !self.caps.contains(&256)
            || self
                .caps
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.caps.len()
        {
            return Err("caps must be unique and include baseline 256".into());
        }
        self.limits.validate()?;
        self.neighborhood.limits.validate()?;
        if self.neighborhood.width == 0
            || self.neighborhood.rounds == 0
            || self.neighborhood.discovery_nodes == 0
        {
            return Err("positive neighborhood configuration required".into());
        }
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Resources {
    pub process_cpu_seconds: Option<f64>,
    pub process_peak_working_set_bytes: Option<u64>,
}
#[cfg(windows)]
pub fn resources() -> Resources {
    use std::ffi::c_void;
    #[repr(C)]
    struct Mem {
        cb: u32,
        faults: u32,
        peak: usize,
        working: usize,
        peak_pool: usize,
        pool: usize,
        peak_nonpaged: usize,
        nonpaged: usize,
        pagefile: usize,
        peak_pagefile: usize,
    }
    #[repr(C)]
    #[derive(Default)]
    struct Ft {
        lo: u32,
        hi: u32,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn GetProcessTimes(p: *mut c_void, a: *mut Ft, b: *mut Ft, c: *mut Ft, d: *mut Ft) -> i32;
    }
    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(p: *mut c_void, m: *mut Mem, sz: u32) -> i32;
    }
    // Win32 fills exactly the declared fixed-size output structures.
    unsafe {
        let p = GetCurrentProcess();
        let mut m: Mem = std::mem::zeroed();
        m.cb = std::mem::size_of::<Mem>() as u32;
        let mem = if GetProcessMemoryInfo(p, &mut m, std::mem::size_of::<Mem>() as u32) != 0 {
            Some(m.peak as u64)
        } else {
            None
        };
        let (mut a, mut b, mut c, mut d) =
            (Ft::default(), Ft::default(), Ft::default(), Ft::default());
        let cpu = if GetProcessTimes(p, &mut a, &mut b, &mut c, &mut d) != 0 {
            Some(
                (((c.hi as u64) << 32 | c.lo as u64) + ((d.hi as u64) << 32 | d.lo as u64)) as f64
                    / 1e7,
            )
        } else {
            None
        };
        Resources {
            process_cpu_seconds: cpu,
            process_peak_working_set_bytes: mem,
        }
    }
}
#[cfg(not(windows))]
pub fn resources() -> Resources {
    Resources {
        process_cpu_seconds: None,
        process_peak_working_set_bytes: None,
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Cell {
    pub name: String,
    pub outcome: Outcome,
    pub cap_profile: Option<search::CapStats>,
    pub incremental_wall_seconds: f64,
    pub incremental_cpu_seconds: Option<f64>,
    /// Process lifetime high-water mark, not an isolated per-method allocation peak.
    pub process_peak_working_set_bytes: Option<u64>,
    pub dependencies: Vec<String>,
}
#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    identity: String,
    sha256: String,
    payload: T,
}
pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_reader(File::open(path).map_err(|e| e.to_string())?)
        .map_err(|e| format!("{}: {e}", path.display()))
}
pub fn write_new<T: Serialize>(path: &Path, x: &T) -> Result<()> {
    if path.exists() {
        return Err(format!("refuse overwrite: {}", path.display()));
    }
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .map_err(|e| e.to_string())?;
    serde_json::to_writer_pretty(&mut file, x).map_err(|e| e.to_string())?;
    file.write_all(b"\n").map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    drop(file);
    fs::rename(&temp, path).map_err(|e| e.to_string())?;
    Ok(())
}
fn save<T: Serialize>(path: &Path, id: &str, x: &T) -> Result<()> {
    write_new(
        path,
        &Envelope {
            identity: id.to_owned(),
            sha256: snapshot::digest(x)?,
            payload: x,
        },
    )
}
fn load<T: Serialize + DeserializeOwned>(path: &Path, id: &str) -> Result<T> {
    let e: Envelope<T> = read_json(path)?;
    if e.identity != id || snapshot::digest(&e.payload)? != e.sha256 {
        return Err(format!(
            "checkpoint identity/digest mismatch: {}",
            path.display()
        ));
    }
    Ok(e.payload)
}
pub fn file_digest(path: &Path) -> Result<String> {
    let mut f = File::open(path).map_err(|e| e.to_string())?;
    let mut sha = Sha256::new();
    let mut buf = vec![0; 1024 * 1024];
    loop {
        let n = f.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        sha.update(&buf[..n]);
    }
    Ok(format!("{:x}", sha.finalize()))
}
/// Portable exports are immutable just like checkpoint cells.
pub fn publish_json(path: &Path, value: &serde_json::Value) -> Result<()> {
    if path.exists() {
        let old: serde_json::Value = read_json(path)?;
        if snapshot::digest(&old)? != snapshot::digest(value)? {
            return Err(format!("portable export mismatch: {}", path.display()));
        }
        Ok(())
    } else {
        write_new(path, value)
    }
}
struct Lock {
    path: PathBuf,
    _file: File,
}
impl Drop for Lock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
fn lock(dir: &Path) -> Result<Lock> {
    let path = dir.join("RUN.lock");
    let mut f=OpenOptions::new().write(true).create_new(true).open(&path).map_err(|e|format!("exclusive run lock {}: {e}; after a crash verify no worker owns it before removing it",path.display()))?;
    writeln!(f, "{}", std::process::id()).map_err(|e| e.to_string())?;
    f.sync_all().map_err(|e| e.to_string())?;
    Ok(Lock { path, _file: f })
}
fn validate_out(v: &View<'_>, b: usize, out: &Outcome) -> Result<()> {
    let evaluated = v.evaluate(&out.book, b)?;
    if evaluated.book != out.book
        || !out.value.is_finite()
        || (evaluated.value - out.value).abs() > 1e-9
        || out
            .bound
            .is_some_and(|x| !x.is_finite() || x + 1e-7 < out.value)
    {
        return Err("checkpoint book/value/budget/bound verification failed".into());
    }
    Ok(())
}
fn cell(
    v: &View<'_>,
    cfg: &Config,
    dir: &Path,
    id: &str,
    name: &str,
    deps: Vec<String>,
    f: impl FnOnce() -> Result<(Outcome, Option<search::CapStats>)>,
) -> Result<Cell> {
    let path = dir.join(format!("{name}.json"));
    if path.exists() {
        let c: Cell = load(&path, id)?;
        if c.name != name || c.dependencies != deps {
            return Err("checkpoint cell mismatch".into());
        }
        validate_out(v, cfg.budget, &c.outcome)?;
        return Ok(c);
    }
    let begin = Instant::now();
    let before = resources();
    let (out, profile) = f()?;
    let after = resources();
    validate_out(v, cfg.budget, &out)?;
    let c = Cell {
        name: name.into(),
        outcome: out,
        cap_profile: profile,
        incremental_wall_seconds: begin.elapsed().as_secs_f64(),
        incremental_cpu_seconds: before
            .process_cpu_seconds
            .zip(after.process_cpu_seconds)
            .map(|(a, b)| (b - a).max(0.)),
        process_peak_working_set_bytes: after.process_peak_working_set_bytes,
        dependencies: deps,
    };
    save(&path, id, &c)?;
    eprintln!(
        "completed {name}: {:.8}, {} prescriptions",
        c.outcome.value,
        c.outcome.book.len()
    );
    Ok(c)
}

#[derive(Serialize, Deserialize)]
pub struct Report {
    pub schema: u32,
    pub identity: String,
    pub graph_nodes: usize,
    pub cycle_cut_nodes: usize,
    pub cells: Vec<Cell>,
    pub tree_root_values: Option<Vec<f64>>,
    pub tree_diagnostic: String,
    pub resource_scope: String,
}
pub fn run(
    snap: &snapshot::Snapshot,
    cfg: &Config,
    dir: &Path,
    code_identity: &str,
) -> Result<Report> {
    let graph = snap.core()?;
    let source = snap.source_book(&graph)?;
    run_core(
        &graph,
        &source,
        &snapshot::digest(snap)?,
        cfg,
        dir,
        code_identity,
    )
}
/// Production-sized graphs remain borrowed in Rust; no graph export is necessary.
pub fn run_core(
    graph: &Core,
    source: &Book,
    graph_identity: &str,
    cfg: &Config,
    dir: &Path,
    code_identity: &str,
) -> Result<Report> {
    cfg.validate()?;
    let view = View::new(graph)?;
    for (&u, &s) in source {
        view.candidate_index(u, s)?;
    }
    fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let _lock = lock(dir)?;
    let binding = serde_json::json!({"schema":1,"graph":graph_identity,"source":source,"config":cfg,"binary":code_identity,
        "compiled_source":env!("OPTIMIZER_CODE_SHA"),"solver":"microlp-0.8.0","feasibility_tolerance":1e-9,"integrality_tolerance":1e-9,
        "prune_epsilon":1e-10,"rounded_policy_value_tolerance":1e-7,"semantics":"fixed-topological-cut-v1"});
    let id = snapshot::digest(&binding)?;
    let identity_path = dir.join("identity.json");
    if identity_path.exists() {
        let old: serde_json::Value = read_json(&identity_path)?;
        if old != binding {
            return Err("resume recipe/input/binary identity drift".into());
        }
    } else {
        write_new(&identity_path, &binding)?;
    }
    let mut cells = Vec::new();
    for &cap in &cfg.caps {
        cells.push(cell(
            &view,
            cfg,
            dir,
            &id,
            &format!("dp-{cap}"),
            vec![],
            || {
                let (o, p) = search::dp_baseline(&view, cfg.budget, cap)?;
                Ok((o, Some(p)))
            },
        )?);
    }
    let price = cell(&view, cfg, dir, &id, "price", vec![], || {
        Ok((search::price_with_source(&view, cfg.budget, &source)?, None))
    })?;
    let dp = cells
        .iter()
        .find(|c| c.name == "dp-256")
        .unwrap()
        .outcome
        .book
        .clone();
    let portfolio = cell(
        &view,
        cfg,
        dir,
        &id,
        "portfolio",
        vec!["dp-256".into(), "price".into()],
        || {
            Ok((
                search::polish_starts(&view, &[dp, price.outcome.book.clone()], cfg.budget)?,
                None,
            ))
        },
    )?;
    cells.push(price);
    cells.push(portfolio.clone());
    let lns = cell(
        &view,
        cfg,
        dir,
        &id,
        "neighborhood",
        vec!["portfolio".into()],
        || {
            let mut start = portfolio.outcome.clone();
            let mut next = 0;
            let mut previous_seconds = 0.;
            let mut previous_states = 0;
            while dir
                .join(format!("neighborhood-step-{next:04}.json"))
                .exists()
            {
                let st: search::NeighborhoodStep =
                    load(&dir.join(format!("neighborhood-step-{next:04}.json")), &id)?;
                validate_out(&view, cfg.budget, &st.outcome)?;
                if st.index != next || st.accepted != (st.outcome.value > start.value + 1e-10) {
                    return Err("neighborhood checkpoint ordering mismatch".into());
                }
                if st.accepted {
                    start = st.outcome.clone();
                }
                previous_seconds += st.outcome.seconds;
                previous_states += st.outcome.states;
                next += 1;
            }
            let mut out = search::improve_from(
                &view,
                cfg.budget,
                &start.book,
                &cfg.neighborhood,
                next,
                |st| {
                    save(
                        &dir.join(format!("neighborhood-step-{:04}.json", st.index)),
                        &id,
                        st,
                    )
                },
            )?;
            out.seconds += previous_seconds;
            out.states += previous_states;
            Ok((out, None))
        },
    )?;
    cells.push(lns);
    if graph.n_our <= cfg.oracle_max_own && graph.n() <= cfg.limits.max_nodes {
        cells.push(cell(&view, cfg, dir, &id, "oracle", vec![], || {
            Ok((exact::enumerate(&view, cfg.budget, &cfg.limits)?, None))
        })?);
    }
    if graph.n() <= cfg.limits.max_nodes {
        cells.push(cell(
            &view,
            cfg,
            dir,
            &id,
            "mip",
            vec!["portfolio".into()],
            || {
                let out = match exact::mip(
                    &view,
                    cfg.budget,
                    &portfolio.outcome.book,
                    None,
                    &cfg.limits,
                ) {
                    Ok(o) => o,
                    Err(e) if e.contains("resource limit") => {
                        let mut o = portfolio.outcome.clone();
                        o.status = e;
                        o.bound = None;
                        o.bound_scope = None;
                        o.states = 0;
                        o.seconds = 0.;
                        o
                    }
                    Err(e) => return Err(e),
                };
                Ok((out, None))
            },
        )?);
    }
    let (tree, diagnostic) = match exact::tree_values(&view, cfg.budget, 2_000_000) {
        Ok(v) => (Some(v), "exact tree budget table".into()),
        Err(e) => (None, e),
    };
    let report=Report{schema:1,identity:id.clone(),graph_nodes:graph.n(),cycle_cut_nodes:view.cuts,cells,
        tree_root_values:tree,tree_diagnostic:diagnostic,
        resource_scope:"Wall/CPU cell increments; add dependency costs for portfolio/LNS end-to-end comparisons. Peak working set is process-lifetime high-water, not isolated per-arm peak. Interrupted cells restart; completed cells and LNS neighborhoods resume.".into()};
    let path = dir.join("report.json");
    if path.exists() {
        let old: Report = load(&path, &id)?;
        if snapshot::digest(&old)? != snapshot::digest(&report)? {
            return Err("completed report mismatch".into());
        }
    } else {
        save(&path, &id, &report)?;
    }
    Ok(report)
}

/// An explicit, unbound queue specification; generating it never launches work.
pub fn study_plan() -> serde_json::Value {
    let mut runs = Vec::new();
    for color in ["white", "black"] {
        for gate in ["stricter", "permissive"] {
            for budget in [2000, 500] {
                runs.push(serde_json::json!({"color":color,"gate":gate,"budget":budget,
            "input_binding":format!("{color}-{gate}-p30"),"phase":if budget==2000{"O2-anchors"}else{"O2-shortlist"},
            "methods":["dp-match-polish","price-polish","portfolio","portfolio-neighborhood"],
            "state":"waiting-for-accepted-inputs-and-calibrated-resource-settings"}));
            }
        }
    }
    serde_json::json!({"schema":1,"decision":"2026-11-25","language":"Rust",
        "backend":"microlp-0.8.0","main_sweep_manifest":"sweep-manifest-2026-11-21-v5.json",
        "main_sweep_manifest_sha256":"36f3e3dbd2c5bb2d639bb0f03610f21c50b56d06b47d22f378dffcac102bb613",
        "changes_main_defaults":false,"calibration_settings":"provisional-until-O1-measured",
        "prerequisites":["native-tests-pass","O1-calibration-accepted","inputs-verified","black-pilot-closed"],
        "required_input_receipts":["candidate-graph","engine-overlay","opponent-policy","stop-values","horizon-and-cuts","price-source-policy"],
        "fixed":{"horizon":30,"eval_weight":0.75,"prior_strength":500,"opponent_policy":"otb-ensemble-v2","stickiness":0,
          "match_inflate":1.5,"absolute_gate_white_score_baselines":{"white":0.5183137990703003,"black":0.5195669627673518}},
        "gates":{"stricter":{"white_absolute":0.05,"black_absolute":0.05,"relative":0.06},
                 "permissive":{"white_absolute":0.110,"black_absolute":0.075,"relative":0.095}},
        "dp_cap_diagnostic":[256,512,1024],"runs":runs,
        "selection":"O3-paired-development-rollouts-quality-versus-total-compute; no winner yet"})
}
