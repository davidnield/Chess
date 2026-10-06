// Stamps the git commit and build date into the binary for `--version` and the
// month mode's provenance. No rerun-if-changed: cargo then reruns this whenever
// a file in the package changes, which is when the commit is likely to have.
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    // Howard Hinnant's days-to-civil algorithm (UTC).
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

fn main() {
    let commit = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".into());
    // Untracked files count: a new source file not yet committed is exactly the
    // case where the commit alone would misdescribe the binary.
    let dirty = git(&["status", "--porcelain", "--", "."]).is_some_and(|s| !s.is_empty());
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let (y, m, d) = civil_from_days((secs / 86_400) as i64);
    println!("cargo:rustc-env=EE_GIT_COMMIT={commit}{}", if dirty { "-dirty" } else { "" });
    println!("cargo:rustc-env=EE_BUILD_DATE={y:04}-{m:02}-{d:02}");
}
