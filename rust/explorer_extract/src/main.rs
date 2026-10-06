//! explorer-extract: see `explorer-extract --help` and the crate README.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use anyhow::{bail, Result};
use clap::{Args, Parser, Subcommand};

use explorer_extract::game::Filters;
use explorer_extract::source::{discover, effective_chunk, parse_month};
use explorer_extract::stats::RunStats;
use explorer_extract::{dump, evals, merge, month, partials, selftest, sys};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "explorer-extract", disable_version_flag = true, about =
    "The explorer extract in Rust: exact against build_pooled_stats.py (d79a0c7) and python-chess 1.11.2.")]
struct Cli {
    /// Print the version, git commit, build date and target features.
    #[arg(long, short = 'V')]
    version: bool,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Drop-in for `build_pooled_stats.py --phase extract` with the explorer flags.
    Partials(PartialsArgs),
    /// Games -> a finished 512-bucket month (EPD at every ply) plus its term monthly.
    Month(MonthArgs),
    /// Per-ply debug rows for the differential test against python-chess.
    DumpPlies(DumpArgs),
    /// Ply-keyed Rust months -> the all-time banded explorer book (exit 0 done,
    /// 1 error or failed gate, 4 out of space, 5 refused).
    Merge(MergeArgs),
    /// The Lichess cloud + fishnet evals -> one eval per book position (exit 0
    /// done, 1 error, 5 refused, 6 short of commit: retry later).
    Evals(EvalsArgs),
    /// The embedded fixtures (keys, SAN table, tokenizer, a mini extract).
    Selftest,
}

/// Flags every mode shares. Only the explorer contract is supported.
#[derive(Args, Clone)]
struct Common {
    #[arg(long, default_value = r"D:\data\chess\standard-chess-games-compressed")]
    source: PathBuf,
    /// Months as Y_M, e.g. 2024_6.
    #[arg(long, num_args = 1.., required = true)]
    months: Vec<String>,
    #[arg(long, num_args = 1..,
          default_values = ["Blitz", "Bullet", "Classical", "Correspondence", "Rapid", "UltraBullet"])]
    events: Vec<String>,
    #[arg(long, default_value_t = 0, allow_negative_numbers = true)]
    min_elo: i32,
    #[arg(long, default_value_t = 30)]
    max_ply: usize,
    #[arg(long, default_value_t = 250_000)]
    chunk_games: u64,
    /// Drop games with a BOT title on either side (the default).
    #[arg(long, overrides_with = "no_exclude_bots")]
    exclude_bots: bool,
    /// Keep bot games.
    #[arg(long)]
    no_exclude_bots: bool,
    #[arg(long, num_args = 0.., default_values = ["Rules infraction", "Abandoned"])]
    exclude_terminations: Vec<String>,
    /// Worker threads (default: all logical cores).
    #[arg(long)]
    threads: Option<usize>,
    /// Run at BELOW_NORMAL priority.
    #[arg(long)]
    below_normal: bool,
    // The explorer contract's own no-ops, accepted so a Python command line works.
    #[arg(long, hide = true)]
    no_prune: bool,
    #[arg(long, hide = true)]
    no_fuse_winpos: bool,
    #[arg(long, hide = true)]
    no_child_eval: bool,
    // Everything else the Python extract can do is refused.
    #[arg(long, hide = true)]
    prune: bool,
    #[arg(long, hide = true)]
    fuse_winpos: bool,
    #[arg(long, hide = true, num_args = 0..)]
    winpos_thresholds: Option<Vec<i32>>,
    #[arg(long, hide = true)]
    crush_hist: bool,
    #[arg(long, hide = true)]
    child_eval: bool,
    #[arg(long, hide = true)]
    max_rating_gap: Option<i32>,
    #[arg(long, hide = true)]
    seed_stats: Option<String>,
}

impl Common {
    fn refuse_non_explorer(&self) -> Result<()> {
        let bad: Vec<&str> = [
            (self.prune || self.seed_stats.is_some(), "--prune/--seed-stats (depth pruning)"),
            (self.fuse_winpos || self.winpos_thresholds.is_some(), "--fuse-winpos/--winpos-thresholds"),
            (self.crush_hist, "--crush-hist"),
            (self.child_eval, "--child-eval"),
            (self.max_rating_gap.is_some(), "--max-rating-gap"),
        ]
        .into_iter()
        .filter(|(on, _)| *on)
        .map(|(_, n)| n)
        .collect();
        if !bad.is_empty() {
            bail!(
                "only the explorer contract is supported (no prune, no winpos, no crush, no \
                 child_eval, no rating-gap filter); refusing {}",
                bad.join(", ")
            );
        }
        Ok(())
    }

    fn filters(&self) -> Filters {
        Filters {
            min_elo: self.min_elo,
            exclude_bots: !self.no_exclude_bots,
            excluded_terminations: self.exclude_terminations.clone(),
        }
    }

    fn setup(&self) -> Result<()> {
        self.refuse_non_explorer()?;
        if self.below_normal {
            sys::set_below_normal().map_err(anyhow::Error::msg)?;
        }
        let n = self.threads.unwrap_or_else(|| {
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
        });
        rayon::ThreadPoolBuilder::new().num_threads(n).build_global()?;
        eprintln!("{} | {n} threads{}", sys::version_line(),
                  if self.below_normal { ", below-normal priority" } else { "" });
        Ok(())
    }

    fn months(&self) -> Result<Vec<(i32, u32)>> {
        self.months.iter().map(|m| parse_month(m)).collect()
    }
}

#[derive(Args)]
struct PartialsArgs {
    #[command(flatten)]
    common: Common,
    #[arg(long, required = true)]
    partial_dir: PathBuf,
    /// Deepest ply given a parent_epd: 16 matches today's extract, 30 is B1's.
    #[arg(long, required = true)]
    epd_max_ply: u32,
}

#[derive(Args)]
struct MonthArgs {
    #[command(flatten)]
    common: Common,
    /// Output root: month=Y_M/bkt=i/part-0000.parquet, _manifest, _conflicts,
    /// _provenance and the _month=Y_M.DONE sentinels.
    #[arg(long, required = true)]
    out: PathBuf,
    /// Where year=Y_month=M.term.parquet goes (default: <out>/_term).
    #[arg(long)]
    term_dir: Option<PathBuf>,
    /// Passes (a power of two dividing --buckets); default: chosen from --mem-gb.
    #[arg(long)]
    passes: Option<u32>,
    /// Memory budget for the tracked maps, GB.
    #[arg(long, default_value_t = 40.0)]
    mem_gb: f64,
    /// Distinct keys per game, for choosing --passes (2024-06: 16.7; 2013: ~22).
    #[arg(long, default_value_t = 22.0)]
    keys_per_game: f64,
    /// Tracked bytes per distinct key, for choosing --passes.
    #[arg(long, default_value_t = month::DEFAULT_BYTES_PER_KEY)]
    bytes_per_key: f64,
    #[arg(long, default_value_t = month::BUCKETS)]
    buckets: u32,
    /// Keep ply in the ps key and end_ply in the term key, so any lower
    /// coverage cap can be derived later (python/ply_cap.py).
    #[arg(long)]
    ply_key: bool,
}

#[derive(Args)]
struct DumpArgs {
    #[command(flatten)]
    common: Common,
    /// Output base path: writes <out>.plies.parquet and <out>.games.parquet.
    #[arg(long, required = true)]
    out: PathBuf,
    #[arg(long, default_value_t = 0)]
    games_per_file: u64,
    /// Also every game whose movetext holds `{`, `(`, `$` or non-ASCII.
    #[arg(long)]
    special: bool,
}

#[derive(Args)]
struct MergeArgs {
    /// The months root (`month --ply-key` output): month=Y_M/bkt=i/, _manifest,
    /// _provenance, _term, _conflicts and the _month=Y_M.DONE sentinels. Read-only.
    #[arg(long, required = true)]
    months_root: PathBuf,
    /// The expected months, every one complete: Y_M values and Y_M..Y_M ranges,
    /// e.g. 2013_1..2026_7.
    #[arg(long, num_args = 1.., required = true)]
    months: Vec<String>,
    /// The book dir.
    #[arg(long, required = true)]
    out: PathBuf,
    /// Where inputs are staged, whole-file, before the merge reads them: an NVMe
    /// dir that is not on the input volume. The tool owns only its bkt*/ and
    /// term/ subdirs, marked by _merge_stage.
    #[arg(long, required = true)]
    stage_dir: PathBuf,
    /// Cap on staged bytes, GB.
    #[arg(long, default_value_t = 200.0)]
    stage_gb: f64,
    /// Buckets to do, e.g. 0-7 or 0-3,9 (default: all 512). Finalize runs once all
    /// 512 and term are done.
    #[arg(long, num_args = 1..)]
    buckets: Option<Vec<String>>,
    /// Up to threads - 1 buckets at once, plus the term merge (default: logical
    /// cores). Not in the settings lock: the output is identical at any count.
    #[arg(long)]
    threads: Option<usize>,
    #[arg(long, default_value_t = 262_144)]
    row_group_rows: usize,
    /// Exit 4 (resumable) when the book's volume would drop below this, GB.
    #[arg(long, default_value_t = 100.0)]
    min_free_gb: f64,
    /// Write these columns without dictionary encoding (the pilot's tuning of
    /// parent_epd, parent_hash, child_hash). In the settings lock.
    #[arg(long, num_args = 1.., value_parser = ["parent_epd", "parent_hash", "child_hash"])]
    no_dictionary: Vec<String>,
    /// Run at BELOW_NORMAL priority.
    #[arg(long)]
    below_normal: bool,
    /// Test only: allow --stage-dir on the input volume.
    #[arg(long, hide = true)]
    test_one_volume: bool,
    /// Test only: exit(86) at PHASE:BUCKET (stage, merge, verify, publish, term, term-publish).
    #[arg(long, hide = true)]
    test_crash_at: Option<String>,
}

#[derive(Args)]
struct EvalsArgs {
    /// The merged explorer book (ps/event=E/elo_band=B/bkt<iii>.parquet). Read-only.
    #[arg(long, required = true)]
    book: PathBuf,
    /// Lichess/chess-position-evaluations: data_*.parquet (+ .ok markers).
    #[arg(long, required = true)]
    cloud: PathBuf,
    /// Lichess/fishnet-evals: standard_rated_YYYY_MM.parquet (+ .ok markers).
    #[arg(long, required = true)]
    fishnet: PathBuf,
    /// Scratch for the bucket shards (e/, c/). Never F:.
    #[arg(long, required = true)]
    work: PathBuf,
    /// The eval DB.
    #[arg(long, required = true)]
    out: PathBuf,
    /// Worker threads. In the settings lock.
    #[arg(long, required = true)]
    threads: usize,
    /// Memory budget, GB (buffers, the bucket semaphore). In the settings lock.
    #[arg(long, required = true)]
    mem_gb: f64,
    /// Output buckets, e.g. 0-7,128 (default: all 512). In the settings lock.
    #[arg(long, num_args = 1..)]
    buckets: Option<Vec<String>>,
    /// Book buckets whose child_hash phase C reads (default: all 512). A subset
    /// makes child-only matches partial (pilot only). In the settings lock.
    #[arg(long, num_args = 1..)]
    child_sources: Option<Vec<String>>,
    /// Phases to run: any of e, c, j (j is followed by finalize). Default: all.
    #[arg(long, value_delimiter = ',', default_value = "e,c,j")]
    phases: Vec<String>,
    /// Run at BELOW_NORMAL priority.
    #[arg(long)]
    below_normal: bool,
    /// Test only: no download/book sentinels, .ok markers or commit check.
    #[arg(long, hide = true)]
    test_inputs: bool,
    /// Test only: exit(86) at PHASE:N (e, c, j, j-publish).
    #[arg(long, hide = true)]
    test_crash_at: Option<String>,
}

fn run_evals(a: EvalsArgs) -> Result<ExitCode> {
    if a.below_normal {
        sys::set_below_normal().map_err(anyhow::Error::msg)?;
    }
    let threads = a.threads.max(1);
    rayon::ThreadPoolBuilder::new().num_threads(threads).build_global()?;
    let all: Vec<u32> = (0..evals::BUCKETS).collect();
    let mut ph = (false, false, false);
    for p in &a.phases {
        match p.as_str() {
            "e" => ph.0 = true,
            "c" => ph.1 = true,
            "j" => ph.2 = true,
            _ => bail!("--phases: unknown phase {p:?} (e, c, j)"),
        }
    }
    let cfg = evals::Config {
        buckets: match &a.buckets { Some(b) => merge::parse_buckets(b)?, None => all.clone() },
        child_sources: match &a.child_sources { Some(b) => merge::parse_buckets(b)?, None => all },
        threads,
        mem_gb: a.mem_gb,
        phases: ph,
        test_inputs: a.test_inputs,
        crash_at: a.test_crash_at.as_deref().map(evals::parse_crash).transpose()?,
        book: a.book,
        cloud: a.cloud,
        fishnet: a.fishnet,
        work: a.work,
        out: a.out,
    };
    eprintln!("{} | evals, {threads} threads{}", sys::version_line(),
              if a.below_normal { ", below-normal priority" } else { "" });
    match evals::run(&cfg) {
        Ok(()) => Ok(ExitCode::SUCCESS),
        Err(e) => {
            let code = evals::exit_code(&e);
            eprintln!("explorer-extract evals: {} (exit {code}): {e:#}",
                      match code { 5 => "refused", 6 => "short of commit", _ => "error" });
            Ok(ExitCode::from(code))
        }
    }
}

fn run_merge(a: MergeArgs) -> Result<ExitCode> {
    if a.below_normal {
        sys::set_below_normal().map_err(anyhow::Error::msg)?;
    }
    let mut no_dict = a.no_dictionary.clone();
    no_dict.sort();
    no_dict.dedup();
    let cfg = merge::Config {
        months: merge::parse_months(&a.months)?,
        buckets: match &a.buckets {
            Some(b) => merge::parse_buckets(b)?,
            None => (0..merge::BUCKETS).collect(),
        },
        threads: a
            .threads
            .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
            .max(1),
        stage_bytes: (a.stage_gb * 1e9) as u64,
        min_free_bytes: (a.min_free_gb * 1e9) as u64,
        row_group_rows: a.row_group_rows.max(1),
        no_dictionary: no_dict,
        allow_one_volume: a.test_one_volume,
        crash_at: a.test_crash_at.as_deref().map(merge::parse_crash).transpose()?,
        months_root: a.months_root,
        out: a.out,
        stage_dir: a.stage_dir,
    };
    match merge::run(&cfg) {
        Ok(()) => Ok(ExitCode::SUCCESS),
        Err(e) => {
            let code = merge::exit_code(&e);
            eprintln!("explorer-extract merge: {} (exit {code}): {e:#}",
                      match code { 4 => "out of space", 5 => "refused", _ => "error" });
            Ok(ExitCode::from(code))
        }
    }
}

fn run() -> Result<ExitCode> {
    let missing = sys::missing_cpu_features();
    if !missing.is_empty() {
        eprintln!(
            "explorer-extract: this build targets x86-64-v3 and this CPU lacks {}. Rebuild \
             without `-C target-cpu=x86-64-v3` in .cargo/config.toml to run here.",
            missing.join(", ")
        );
        return Ok(ExitCode::from(3));
    }
    let cli = Cli::parse();
    if cli.version {
        println!("{}", sys::version_line());
        return Ok(ExitCode::SUCCESS);
    }
    let Some(cmd) = cli.cmd else {
        bail!("no subcommand; see --help");
    };
    match cmd {
        Cmd::Selftest => {
            let t0 = Instant::now();
            let mut bad = 0;
            for (name, r) in selftest::run_embedded() {
                match r {
                    Ok(s) => println!("  PASS  {name}: {s}"),
                    Err(e) => {
                        bad += 1;
                        println!("  FAIL  {name}: {e}");
                    }
                }
            }
            println!(
                "\n{} ({:.2}s) -- {}",
                if bad == 0 { "SELFTEST PASS".to_string() } else { format!("{bad} FAILURES") },
                t0.elapsed().as_secs_f64(),
                sys::version_line()
            );
            Ok(if bad == 0 { ExitCode::SUCCESS } else { ExitCode::FAILURE })
        }
        Cmd::Partials(a) => {
            a.common.setup()?;
            let files = discover(&a.common.source, &a.common.months()?, &a.common.events)?;
            if files.is_empty() {
                bail!("no source files for {:?} under {}", a.common.months, a.common.source.display());
            }
            let cfg = partials::Config {
                partial_dir: a.partial_dir.clone(),
                epd_max_ply: a.epd_max_ply,
                max_ply: a.common.max_ply,
                chunk: effective_chunk(a.common.chunk_games),
                filters: a.common.filters(),
                events: a.common.events.clone(),
                chunk_games_arg: a.common.chunk_games,
            };
            let stats = RunStats::default();
            let s = partials::run(&cfg, &files, &stats)?;
            let c = stats.get();
            eprintln!(
                "\npartials done: {} files ({} skipped), {} chunks written, {} ps rows, {:.1} MB \
                 in {:.1} min\n  games {} kept {} failed {} null-move tokens {} plies {} | dropped: \
                 elo {} no_score {} termination {} bot {}\n  {:.0} games/s, {:.2} us/ply (wall, all threads)",
                s.files, s.skipped, s.chunks_written, s.ps_rows, s.bytes as f64 / 1e6, s.secs / 60.0,
                c.games, c.kept, c.failed, c.null_tokens, c.plies, c.drop_elo, c.drop_no_score,
                c.drop_termination, c.drop_bot, c.games as f64 / s.secs.max(1e-9),
                s.secs * 1e6 / (c.plies.max(1) as f64)
            );
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Month(a) => {
            a.common.setup()?;
            if !a.buckets.is_power_of_two() {
                bail!("--buckets must be a power of two, got {}", a.buckets);
            }
            if a.common.max_ply == 0 || a.common.max_ply > 1000 {
                bail!("--max-ply must be in 1..=1000, got {}", a.common.max_ply);
            }
            let threads = rayon::current_num_threads();
            let flags = serde_json::json!({
                "source": a.common.source, "months": a.common.months, "events": a.common.events,
                "min_elo": a.common.min_elo, "max_ply": a.common.max_ply,
                "chunk_games": a.common.chunk_games, "exclude_bots": !a.common.no_exclude_bots,
                "exclude_terminations": a.common.exclude_terminations, "threads": threads,
                "below_normal": a.common.below_normal, "out": a.out, "term_dir": a.term_dir,
                "passes": a.passes, "mem_gb": a.mem_gb, "keys_per_game": a.keys_per_game,
                "bytes_per_key": a.bytes_per_key, "buckets": a.buckets, "ply_key": a.ply_key,
            });
            let cfg = month::Config {
                out: a.out.clone(),
                term_dir: a.term_dir.clone().unwrap_or_else(|| a.out.join("_term")),
                passes: a.passes,
                mem_gb: a.mem_gb,
                keys_per_game: a.keys_per_game,
                bytes_per_key: a.bytes_per_key,
                max_ply: a.common.max_ply,
                chunk: effective_chunk(a.common.chunk_games),
                filters: a.common.filters(),
                events: a.common.events.clone(),
                buckets: a.buckets,
                threads,
                ply_key: a.ply_key,
                flags,
            };
            for (y, m) in a.common.months()? {
                let files = discover(&a.common.source, &[(y, m)], &a.common.events)?;
                if files.is_empty() {
                    bail!("no source files for {y}_{m} under {}", a.common.source.display());
                }
                month::run_month(&cfg, y, m, &files)?;
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::DumpPlies(a) => {
            a.common.setup()?;
            let files = discover(&a.common.source, &a.common.months()?, &a.common.events)?;
            let cfg = dump::Config {
                out: a.out.clone(),
                games_per_file: a.games_per_file,
                special: a.special,
                max_ply: a.common.max_ply,
                filters: a.common.filters(),
            };
            let t0 = Instant::now();
            let (ng, np) = dump::run(&cfg, &a.common.source, &files)?;
            eprintln!("dumped {ng} games, {np} plies from {} files ({:.1}s)", files.len(),
                      t0.elapsed().as_secs_f64());
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Merge(a) => run_merge(a),
        Cmd::Evals(a) => run_evals(a),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("explorer-extract: error: {e:#}");
            ExitCode::FAILURE
        }
    }
}
