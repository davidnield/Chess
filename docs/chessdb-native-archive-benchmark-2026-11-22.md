# ChessDB native streaming and NAS benchmark — 2026-11-22

Completed. Rust is faster than the Python conversion prototype. The tested architecture is the existing
C++/TerarkDB source reader feeding framed records directly into a Windows Rust/Arrow/Parquet writer.
No intermediate full-database dump or global sort is required. No full archive export has started.

## Conversion comparison

The same 3,000,000 records from the prior 12-region pilot were converted with both implementations,
alternating execution order by region. Both use the same decoded schema, original binary columns,
32,768-row groups, delta-byte-array key/FEN encoding, no dictionaries, and the selected Zstd level.
Each output was read back and compared field-for-field with the reference frames.

| Compression | Python write time | Rust write time | Writer speedup | Rust sample bytes |
|---|---:|---:|---:|---:|
| Zstd 3 | 17.556 s | 4.463 s | **3.93x** | 97,414,318 |
| Zstd 9 | 23.581 s | 9.480 s | **2.49x** | 90,728,089 |

Rust's file sizes differ by only about 0.03% from PyArrow. The Zstd-9 readable-plus-original archive still
projects to approximately **1.94 TB**. These remain coverage-sample projections, not a census guarantee.
Rust timings also include the logical-source digest and an explicit durable flush. Python and Rust use
different native Parquet implementations; this measures the complete tested writer designs, not language
speed in isolation. It does not imply a 2.5x improvement for every part of a full archive job.

The faster C++ reader replaces stringstream-based binary-to-hex conversion with a direct lookup table and
decodes only the canonical FEN needed by this schema. It uses the same pinned ChessDB value decoder.
The streaming tests compare its readable prefixes with the old reader's outputs before accepting them.

## Streaming tests

These runs read the live read-only snapshot, decode its records, pipe directly into Rust, and durably write
local Parquet. They use one million records per worker. More workers therefore also introduces additional
source regions; this is a practical throughput check, not a fixed-workload controlled scaling experiment.

| Concurrent workers | Total records | Export time | Aggregate records/s | Arithmetic full-export duration |
|---|---:|---:|---:|---:|
| 1 | 1,000,000 | 6.48 s | 154,410 | 115.1 h |
| 2 | 2,000,000 | 7.39 s | 270,508 | 65.7 h |
| 4 | 4,000,000 | 12.98 s | 308,163 | 57.7 h |
| 8 | 8,000,000 | 24.62 s | 324,906 | 54.7 h |

Every exported original key/value pair was reconstructed from Parquet and compared against a fresh source
iterator: **15 million record comparisons across the four arms**, including repeat ranges. Each stream's
first 250,000 decoded rows also matched the old independently produced frames exactly. The streaming output
digest matched the Parquet content digest. Fixture tests passed round trips and rejected truncated data,
too few records, and excess records without publishing a final Parquet file.

The full-export durations are simple extrapolations, not scheduled ETAs: the samples are small, some source
pages are cached, startup/seek costs are included, key-region complexity varies, and future complete metadata
columns will change the workload. Verification and NAS transfers are additional. The prototype verifier uses
Python reconstruction (7–50 seconds per test arm) and must also become a native streaming verifier for production.
WSL reports 12 CPUs with no cgroup CPU quota; no WSL settings were changed.

## NAS measurements

Destination: `\\DyerNAS\data\chess`. Reported free space before tests: **24.63 TB**.
The Windows Ethernet adapter reports **1 Gbps**; the NAS SMB connection uses local addresses
192.168.68.50 -> 192.168.68.58. Access to detailed SMB connection settings was denied by Windows; no settings
or credentials were changed. The cause of the low transfer rate has not been established.

All tests used already-compressed Parquet bytes, repeated to form bounded transfer files. Each NAS output
passed a SHA-256 read-back check. Reads may be cached; these are short tests, not sustained multi-TB promises.

| Test | Data | Write/copy speed | Read-and-hash speed |
|---|---:|---:|---:|
| Python, one file, flush included | 1 GiB | 5.31 MB/s | 8.58 MB/s |
| Python, four parallel files | 512 MiB total | **11.43 MB/s** | **10.32 MB/s** |
| Native Windows robocopy, unbuffered `/J` | 256 MiB | 7.25 MB/s | 6.63 MB/s |

Robocopy exited 1 (successful copy), not a failure. Parallel copying helped, but changing to a native copier
did not remove the bottleneck. Current best rates imply roughly **47 hours to write** and **52 hours to read
back** a 1.94-TB archive, if sustained separately. Simultaneous transfer/verification may contend for the same
link and disks; do not assume perfect overlap. Low throughput is not a lack of NAS capacity.

## Recommendation and production prerequisites

1. Use the **native C++ reader plus Rust writer**, preserving the known database decoder rather than rewriting
   the specialized database reader in Rust. Four export workers are a reasonable starting point: eight improved
   measured throughput only about 5%, and should not be assumed optimal without a longer fixed-workload trial.
2. Keep Zstd 9 as the archive candidate: its extra compute trades for approximately 140 GB less NAS traffic
   versus Zstd 3. The writer is faster in Rust, but full-pipeline compression-level tuning remains possible.
3. Investigate NAS/network throughput before scheduling the full archive. Both sequential and native-copy tests
   were slow. Rates near 10–11 MB/s are consistent with a 100-Mbps bottleneck somewhere, but this is an inference,
   not a verified diagnosis of the NAS port, switch, cable, or storage system.
4. Implement a maintained full exporter with complete nonoverlapping key-range coverage, all raw records,
   both orientations' distance metadata, unknown-record preservation, immutable source/code identity, native
   source/output digest verification, resumable shard receipts, and a final coverage manifest. The benchmark's
   bounded prefix sampler is **not** such an exporter.
5. Use a bounded local spool, then verified NAS uploads; throttle conversion if uploads fall behind. Do not
   require 2 TB of new local output or retain a full framed intermediate dump. Preserve the existing free-space
   reserves. Keep both raw copies until full archive acceptance and NAS verification.

At the tested rates, allow several days for conversion, transfer and complete verification; the approximately
55–60-hour conversion projection and approximately 99-hour NAS write-plus-read projection are different
components, with only partial overlap possible. No production start time or guaranteed completion ETA is set.

## Artifacts and reproducibility

All runtime artifacts are under `D:/chess/chessdb-archive-native-20261122-v1`:

- `BENCHMARK.COMPLETE.json`, `file-comparison.json`, `stream-scaling.json`, `rust-fixture-tests.json`;
- `nas-benchmark.json`, `nas-parallel.json`, `nas-robocopy.json`, and `robocopy.log`;
- immutable benchmark executable, reader, source/build scripts, Cargo lockfile and source archive;
- each output's logical digest and range endpoints, source-verification frames and checksums;
- `native-bench.log`, `native-status.json`, and reader provenance.

Rust binary SHA-256: `8ad77e9091adc5e5a5d02026b0aa879e6449886bdeee15386ab6ac42120c69f6`.
Reader binary SHA-256: `2a1222cf59b4d225dc88ef5b6f59a348d1ac18b02303eb9f90be5124d7c18087`.

NAS benchmark files are explicitly labeled under `chessdb-archive-benchmark-20261122-v1`; they occupy about
1.75 GiB and are not the archive. All benchmark processes have completed. Original ChessDB files, canonical
books, active policy bindings, and existing accepted artifacts remain unchanged.


## Follow-up: F: instead of NAS

David asked whether the archive should stay on F: instead. F: is the 16-TB Seagate Expansion USB HDD,
formatted exFAT, and had 3.1246 TB free before testing. The measured ~1.94-TB archive would leave about
1.18 TB free. This use matches F:'s existing archive role; no live sorting/temp workload is proposed.

A 1-GiB test of already-compressed data measured **117.0 MB/s writing including flush** and **136.2 MB/s
reading plus SHA-256 verification with Windows FILE_FLAG_NO_BUFFERING**. Both hashes matched. An ordinary
cached read measured 404.6 MB/s and is explicitly NOT the disk-throughput estimate. The unbuffered result
bypasses Windows file cache, though drive/controller caching and short-test effects still apply.

Compared with the best NAS measurements, F: is about 10x faster for writing and 13x faster for verified reading.
At the measured rates, 1.94 TB corresponds to about 4.6 hours of writes plus 4.0 hours of read-back, versus
approximately 47+52 hours on the NAS. These phases may contend if overlapped; multi-TB sustained rates can differ.
Conversion remains the dominant measured cost (~55-60 hours extrapolated), and native full verification and
production checkpoint/coverage work remain necessary.

**Updated recommendation:** prefer F: as the primary archive destination, with an optional NAS copy later.
This removes NAS performance investigation from the critical path if David chooses F:. The full exporter is
still unstarted, and neither original copy has been deleted. Enforce a free-space reserve, use bounded shards,
and accept the complete verified archive before reclaiming originals. Existing extracted months on F: are retained.

Receipts: `f-drive-benchmark.json` and `f-drive-unbuffered-read.json` under the native benchmark directory.
The labeled F: benchmark occupies 1 GiB at `F:/chess/chessdb-archive-benchmark-20261122-v1`.
