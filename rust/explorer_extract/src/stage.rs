//! `merge`'s staging: every read of the month tree (a USB spinning disk) is a
//! whole-file sequential copy, made by ONE thread, into `--stage-dir` on NVMe.
//! The merge workers read only the stage.
//!
//! * Order: the term files first (`<stage>/term/`), then each bucket's month
//!   files in month order (`<stage>/bkt<iii>/month=Y_M.parquet`), bucket by
//!   bucket ahead of the workers.
//! * Budget: staged bytes never exceed `--stage-gb`, except that a bucket (or
//!   the term files) alone on the stage may finish staging past it, so a cap
//!   below one bucket cannot deadlock.
//!   A bucket's bytes are released once its sentinel is written and its stage
//!   deleted.
//! * Ownership: the tool owns only `<stage>/bkt*/` and `<stage>/term/`, marked
//!   by a `_merge_stage` file it creates. A stage dir holding anything else, or
//!   non-empty without the marker, is refused before anything is deleted: a
//!   mistyped `--stage-dir D:\chess` must never delete anything.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};

pub const MARKER: &str = "_merge_stage";
/// Copy buffer: whole-file copies in 16 MB reads (the spec asks for >= 8 MB).
pub const COPY_BUF: usize = 16 << 20;

/// `bkt` + three digits, the name of a bucket's stage (and output) subdir.
pub fn bucket_dir_name(b: u32) -> String {
    format!("bkt{b:03}")
}

fn is_bucket_dir_name(s: &str) -> bool {
    s.len() == 6 && s.starts_with("bkt") && s[3..].bytes().all(|c| c.is_ascii_digit())
}

/// What a stage dir may hold before the tool deletes anything in it.
#[derive(Debug, PartialEq, Eq)]
pub enum StageCheck {
    /// Absent or empty: the marker is created.
    Fresh,
    /// Ours: the marker plus only bkt*/ and term/ subdirs.
    Owned { subdirs: Vec<PathBuf> },
    /// Anything else: refuse, delete nothing.
    Foreign(String),
}

pub fn inspect_stage_dir(dir: &Path) -> Result<StageCheck> {
    if !dir.exists() {
        return Ok(StageCheck::Fresh);
    }
    if !dir.is_dir() {
        return Ok(StageCheck::Foreign(format!("{} is not a directory", dir.display())));
    }
    let mut marker = false;
    let mut subdirs = Vec::new();
    let mut other = Vec::new();
    for e in std::fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        let ft = e.file_type()?;
        if name == MARKER && ft.is_file() {
            marker = true;
        } else if ft.is_dir() && (name == "term" || is_bucket_dir_name(&name)) {
            subdirs.push(e.path());
        } else {
            other.push(name);
        }
    }
    if !marker {
        if subdirs.is_empty() && other.is_empty() {
            return Ok(StageCheck::Fresh);
        }
        let mut names: Vec<String> = other;
        names.extend(subdirs.iter().map(|p| p.file_name().unwrap().to_string_lossy().into_owned()));
        names.sort();
        return Ok(StageCheck::Foreign(format!(
            "{} is not empty and has no {MARKER} marker (holds {:?}{})",
            dir.display(),
            &names[..names.len().min(5)],
            if names.len() > 5 { ", ..." } else { "" }
        )));
    }
    if !other.is_empty() {
        other.sort();
        return Ok(StageCheck::Foreign(format!(
            "{} holds files the tool does not own: {:?}{}",
            dir.display(),
            &other[..other.len().min(5)],
            if other.len() > 5 { ", ..." } else { "" }
        )));
    }
    subdirs.sort();
    Ok(StageCheck::Owned { subdirs })
}

/// Make `dir` ours and empty of stage subdirs. Returns Err(message) for a dir
/// the tool must refuse (nothing has been deleted then).
pub fn prepare_stage_dir(dir: &Path) -> Result<std::result::Result<usize, String>> {
    match inspect_stage_dir(dir)? {
        StageCheck::Foreign(why) => Ok(Err(why)),
        StageCheck::Fresh => {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
            std::fs::write(
                dir.join(MARKER),
                "explorer-extract merge stage dir: the tool deletes bkt*/ and term/ here on start.\n",
            )?;
            Ok(Ok(0))
        }
        StageCheck::Owned { subdirs } => {
            for s in &subdirs {
                std::fs::remove_dir_all(s).with_context(|| format!("removing {}", s.display()))?;
            }
            Ok(Ok(subdirs.len()))
        }
    }
}

/// Copy `src` to `dst` whole, sequentially, in `buf`-sized reads; the copy's
/// size must equal the source's.
pub fn copy_whole(src: &Path, dst: &Path, buf: &mut [u8]) -> Result<u64> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;
        opts.custom_flags(FILE_FLAG_SEQUENTIAL_SCAN);
    }
    let mut f = opts.open(src).with_context(|| format!("opening {}", src.display()))?;
    let want = f.metadata()?.len();
    let mut out = File::create(dst).with_context(|| format!("creating {}", dst.display()))?;
    let mut n = 0u64;
    loop {
        let k = f.read(buf).with_context(|| format!("reading {}", src.display()))?;
        if k == 0 {
            break;
        }
        out.write_all(&buf[..k]).with_context(|| format!("writing {}", dst.display()))?;
        n += k as u64;
    }
    out.flush()?;
    drop(out);
    let got = std::fs::metadata(dst)?.len();
    if n != want || got != want {
        bail!("copy of {} is {got} bytes ({n} read), the source {want}", src.display());
    }
    Ok(n)
}

// ── the queue between the stager and the workers ────────────────────────────

/// One input file on the stage: (month index, staged path, bytes).
pub type StagedFile = (usize, PathBuf, u64);

pub struct Staged {
    /// A bucket number, or None for the term files.
    pub bucket: Option<u32>,
    pub dir: PathBuf,
    pub files: Vec<StagedFile>,
    pub bytes: u64,
    /// Seconds spent copying (budget waits excluded).
    pub secs: f64,
}

/// Why the run is stopping: an exit status and the message.
#[derive(Clone, Debug)]
pub struct Stop {
    pub code: u8,
    pub msg: String,
}

#[derive(Default)]
struct QState {
    used: u64,
    ready: BTreeMap<u32, Staged>,
    term: Option<Staged>,
    stager_done: bool,
    stop: Option<Stop>,
    copied_bytes: u64,
    copy_secs: f64,
}

pub struct Queue {
    cap: u64,
    st: Mutex<QState>,
    cv: Condvar,
}

impl Queue {
    pub fn new(cap: u64) -> Queue {
        Queue { cap, st: Mutex::new(QState::default()), cv: Condvar::new() }
    }

    /// Reserve `need` staged bytes for a group that already holds `own`,
    /// waiting for room. When nothing but that group is staged it may proceed
    /// past the cap (a group larger than the cap would otherwise wait for
    /// itself). False if the run stopped.
    pub fn reserve(&self, need: u64, own: u64) -> bool {
        let mut s = self.st.lock().unwrap();
        loop {
            if s.stop.is_some() {
                return false;
            }
            if s.used == own || s.used + need <= self.cap {
                s.used += need;
                return true;
            }
            s = self.cv.wait(s).unwrap();
        }
    }

    pub fn release(&self, bytes: u64) {
        let mut s = self.st.lock().unwrap();
        s.used = s.used.saturating_sub(bytes);
        self.cv.notify_all();
    }

    pub fn copied(&self, bytes: u64, secs: f64) {
        let mut s = self.st.lock().unwrap();
        s.copied_bytes += bytes;
        s.copy_secs += secs;
    }

    pub fn push(&self, staged: Staged) {
        let mut s = self.st.lock().unwrap();
        match staged.bucket {
            Some(b) => {
                s.ready.insert(b, staged);
            }
            None => s.term = Some(staged),
        }
        self.cv.notify_all();
    }

    pub fn stager_finished(&self) {
        let mut s = self.st.lock().unwrap();
        s.stager_done = true;
        self.cv.notify_all();
    }

    /// The lowest staged bucket, waiting for one; None once the stager is done
    /// and nothing is left, or the run stopped.
    pub fn next_bucket(&self) -> Option<Staged> {
        let mut s = self.st.lock().unwrap();
        loop {
            if s.stop.is_some() {
                return None;
            }
            if let Some(&b) = s.ready.keys().next() {
                return s.ready.remove(&b);
            }
            if s.stager_done {
                return None;
            }
            s = self.cv.wait(s).unwrap();
        }
    }

    /// The staged term files, waiting for them; None if the run stopped or the
    /// stager finished without staging term.
    pub fn take_term(&self) -> Option<Staged> {
        let mut s = self.st.lock().unwrap();
        loop {
            if s.stop.is_some() {
                return None;
            }
            if let Some(t) = s.term.take() {
                return Some(t);
            }
            if s.stager_done {
                return None;
            }
            s = self.cv.wait(s).unwrap();
        }
    }

    /// Stop the run (the first reason wins).
    pub fn stop(&self, code: u8, msg: String) {
        let mut s = self.st.lock().unwrap();
        if s.stop.is_none() {
            s.stop = Some(Stop { code, msg });
        }
        self.cv.notify_all();
    }

    pub fn stopped(&self) -> Option<Stop> {
        self.st.lock().unwrap().stop.clone()
    }

    /// (staged bytes held, buckets staged and waiting, bytes copied, copy secs).
    pub fn snapshot(&self) -> (u64, usize, u64, f64) {
        let s = self.st.lock().unwrap();
        (s.used, s.ready.len(), s.copied_bytes, s.copy_secs)
    }

    /// Wait up to `d` for any change (used by the progress loop).
    pub fn wait_a_while(&self, d: Duration) {
        let s = self.st.lock().unwrap();
        let _ = self.cv.wait_timeout(s, d).unwrap();
    }
}

/// One thing the stager copies: its source and its name on the stage.
pub struct CopyJob {
    pub month: usize,
    pub src: PathBuf,
    pub name: String,
}

/// Copy one group of files (a bucket's, or the term files) onto the stage,
/// reserving budget file by file. None if the run stopped meanwhile.
pub fn stage_group(
    q: &Queue,
    bucket: Option<u32>,
    dir: &Path,
    jobs: &[CopyJob],
    buf: &mut [u8],
    mut after_file: impl FnMut(usize),
) -> Result<Option<Staged>> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let mut files = Vec::with_capacity(jobs.len());
    let (mut bytes, mut secs) = (0u64, 0f64);
    for (k, j) in jobs.iter().enumerate() {
        let size = std::fs::metadata(&j.src)
            .with_context(|| format!("{}: missing", j.src.display()))?
            .len();
        if !q.reserve(size, bytes) {
            q.release(bytes);
            return Ok(None);
        }
        let dst = dir.join(&j.name);
        let t = Instant::now();
        let n = match copy_whole(&j.src, &dst, buf) {
            Ok(n) => n,
            Err(e) => {
                q.release(bytes + size);
                return Err(e);
            }
        };
        let dt = t.elapsed().as_secs_f64();
        q.copied(n, dt);
        if n != size {
            q.release(bytes + size);
            bail!("{} changed size while staging ({size} -> {n})", j.src.display());
        }
        bytes += n;
        secs += dt;
        files.push((j.month, dst, n));
        after_file(k);
    }
    Ok(Some(Staged { bucket, dir: dir.to_path_buf(), files, bytes, secs }))
}
