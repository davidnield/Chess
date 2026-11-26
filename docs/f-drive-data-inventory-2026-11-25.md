# F: data value, dependencies and cleanup candidates

Read-only audit on November 25, 2026, following the successful conditional
mark-clean pass. **No files were deleted, moved or rewritten on F:.**

The inventory enumerated **176,067 files and 168,211 directories**, with no
enumeration errors, in 306 seconds. It found **14,920,575,764,154 bytes** of file
contents. Sizes below are decimal TB/GB; Windows usually displays binary units.
This is a metadata inventory and dependency review, not another full byte audit
or proof that a second copy is complete. Allocated space also includes cluster
rounding and directory structures.

## Keep and prioritize for backup

| F: path | File bytes | Purpose and consequence of loss |
|---|---:|---|
| `chess/chessdb-archive-20260702-v6-zstd3` | 2,093,396,391,288 (2.09 TB including receipts) | The only full retained ChessDB snapshot found in the documented local/NAS locations. The two native raw payload copies were deliberately retired after archive acceptance. This holds all 63,996,865,341 source records, including original key/value bytes; the active Rust archive reader queries it. Existing D:/E: lookups and menus survive, but they cannot reconstruct the full archive. Highest priority for a second independently verified copy. |
| `chess/explorer_brc_p51_pilot` | 8,032,782,244,716 (8.03 TB) | Despite the name, this is the **full production monthly archive**, not a disposable pilot. It contains all 163 training months, January 2013–July 2026, 128-bit keys, per-ply counts through ply 51, and sliced termination evidence for Blitz/Rapid/Classical. It is upstream of the accepted E: explorer. Losing it costs the monthly time-series artifact and the inexpensive merge/rebuild source. It can be re-extracted if the original compressed games on D: and producer/provenance remain, but that is substantial repeat work. |

The E: merged explorer is a downstream all-time aggregate, **not a lossless
backup of the monthly archives**: its aggregation no longer preserves which
month contributed each move count. It does retain per-ply, speed and rating
information, so current pools and many future pool changes can use it without F:.

Known duplicate locations checked: the H: native ChessDB payload and WSL copy
are retired as documented in the earlier acceptance report. The D: extraction
stage has no `month=*` directories and no remaining staged provenance JSONs;
it does retain 163 small source inventories. The known NAS location
`\\DyerNAS\data\chess` contains only `chessdb-archive-benchmark-20261122-v1`,
not a production archive. This is not an inventory of every possible backup
location the user may have configured elsewhere.

## Legacy, but not safe to classify as wholly redundant

`F:/chess/explorer_banded/rust_p30k` occupies **3,987,019,918,443 bytes (3.99 TB)**.
It is the older 64-bit, ply-30 archive for the same 163 months. Its BRC role is
superseded by the richer 128-bit archive. Two dependencies prevent blanket deletion:

1. It includes **Bullet, UltraBullet and Correspondence**, as well as BRC. The
   new extraction contains only BRC, so it is not a full population replacement.
   The current Opening Dashboard's `python/extract_opening_tree.py:43` still
   reads this archive; `python/gate_stage_a.py:27` reads its manifests. Existing
   dashboard outputs on E: survive a failure, but fresh extraction or expanded
   historical queries would need migration or re-extraction.
2. The current migration launcher still reads its **`_provenance`** directory
   to check source partition inventories and distinguish legitimately absent
   historical speeds from missing data. See `python/run_ply51_migration.py:23`
   and `python/month_archive.py:86`. The 163 provenance files total only
   **811,990 bytes**. The 163 monthly manifest files total **1,422,664 bytes**.
   Preserve these, the extraction parameters and month completion markers even
   if the bulk Parquet data is retired later.

Recommendation: treat the 3.99 TB as a **conditional retirement candidate**.
First decide whether to preserve the dashboard's six-speed scope, migrate its
readers if appropriate, and retain the small independent provenance. No migration
or scope change was made in this audit. The dashboard also retains some stale
E: paths to the deleted old merged book; an old path reference alone does not
establish that the entire historical rebuild currently runs successfully.

## Better cleanup candidates

| Contents | File bytes | Assessment |
|---|---:|---|
| `chess/position-stats/_pooled_partials_ge1800_2013_2026_brc` | 753,569,050,313 (753.57 GB) | 326 legacy monthly position/terminal partial files. These support rebuilding the old 1800+ pool and exact historical recipe work. Neither current pool queue reads them. The richer new monthly archive and retained games provide a route to recomputation, but byte-identical equivalence to the legacy recipe was not tested here. This is the strongest large cleanup candidate if exact old rebuilds are no longer required; preserve manifests/control samples first. |
| Old ChessDB v1/v2 roots and all test/speed/drive benchmark folders, excluding v6 | 17,918,666,289 (17.92 GB) | Superseded partial generations and bounded test outputs. v1 and v2 are already down to about 10 MB each after prior payload cleanup; they are not additional TB-sized copies. The full v6 archive is the accepted replacement. Preserve small test receipts/reports rather than treating these outputs as backups. |
| Recycle Bin's old `_to_delete_2026-10-25` | 35,855,097,973 (35.86 GB including recycling metadata) | Already quarantined/deleted work. Contains 6.40 GB of old DuckDB spill, a 29.22 GB September 2026 raw PGN archive, a 231 MB backfill fragment, and small manifests/shared files. The spill is disposable; the raw PGN needs the qualification below. |
| `chess_duckdb_tmp` and `msdownld.tmp` at the drive root | 0 file bytes | Empty. No useful capacity to reclaim. |

The recycled PGN is
`$RECYCLE.BIN/$R3F8UI2/chess/_backfill_work/lichess_db_standard_rated_2026-09.pgn.zst`.
It is not a current pipeline input. However, raw PGNs may preserve clock/evaluation
comments that the compressed game corpus strips; this audit did not decompress
it or prove a byte-identical duplicate exists. **Do not call the entire Recycle
Bin verified redundant merely because it is recycled.** Its metadata identifies
the original deleted directory as `F:\_to_delete_2026-10-25`.

The approximately 17.92 GB of old ChessDB artifacts breaks down as follows:

| Folder suffix under `F:/chess` | Decimal GB |
|---|---:|
| `chessdb-speed-benchmark-20261123-v4` | 15.168 |
| `chessdb-speed-benchmark-20261123-v3` | 1.230 |
| `chessdb-archive-benchmark-20261122-v1` | 1.074 |
| `chessdb-archive-tests-20261122-v1`, `-v4`, `-v5` combined | 0.427 |
| `chessdb-archive-20260702-v1`, `-v2`, speed smoke/empty folders | 0.020 |

Root Recovery/Seagate/System Volume Information metadata is negligible and is
not proposed for deletion. The bulk original Lichess raw archive was already
deleted on November 14; it is not an additional 5.4 TB cleanup opportunity now.

## Immediate impact of an F: failure

- Current canonical repertoires and trainer inputs on E: remain.
- The accepted 4.50 TB merged ply-51 explorer on E: remains; both active pool
  queues consume that generation. Main working/output paths remain D:/E:/H:;
  mini staging/collection remains on the main PC and the mini.
- Published eval DB/lookups on D: and move menus on E: remain. The menus select
  explorer positions with at least two observations and the lookups are derived
  subsets, so neither is a replacement for all ChessDB records.
- Direct queries through `D:/chess/bin/chessdb-archive-query-20261125-v1` fail
  when its F: archive is unavailable. New source-wide imports cannot be repeated
  without restoring/reacquiring the archive.
- Monthly explorer rebuilds, alternate date-window aggregation and legacy
  dashboard extraction lose their F: source. Original compressed games on D:
  make re-extraction possible, provided those games and the required code and
  provenance are themselves preserved.

## Recommended priority

1. Preserve the small source provenance, manifests, archive receipts and frozen
   tool bindings on another device, and make a **second verified ChessDB archive
   copy** the first bulk backup priority. F: currently serves as its primary
   archive, not as a redundant backup.
2. Protect the compressed game source on D: and choose whether the 8.03 TB monthly
   archive also merits a full backup versus accepting re-extraction cost.
3. Consider retiring the 754 GB legacy partials. Handle the 3.99 TB six-speed
   explorer separately, with an explicit dashboard/population decision.
4. Small benchmark/spill cleanup is reasonable housekeeping but does little to
   address the loss risk. Copying data elsewhere on F: would not add protection
   against failure of F: itself. Recheck destination reserves and sustained
   transfer speed before scheduling any multi-TB backup.

No copy, deletion, migration or new recurring audit is armed by this report.

## Evidence

- `docs/evidence/f-drive-data-inventory-20261125/inventory.json`: complete
  directory aggregates and enumeration scope/errors.
- `docs/evidence/f-drive-data-inventory-20261125/inventory.py`: the metadata-only
  traversal, with eight workers and no writes to F:.
- `E:/chess/position-stats/brc-p51-train-201301-202607-20261115/README.md` and
  `_merge_params.json`: production monthly source, three speeds and ply-51 keys.
- Both old/new F: `_extract_params.json` files; 163 month markers in each.
- `docs/chessdb-archive-lookup-retirement-2026-11-25.md`: accepted archive and
  verified native-copy retirement.
- `docs/f-drive-investigation-2026-11-25.md`: successful filesystem mark-clean
  and subsequent inventory/sample-hash/971-query checks.

F:'s November 14 README predates the new archive and ChessDB reader and still
calls F: archives-only/never an input. That description is stale; the concrete
dependencies above take precedence for this inventory.
