# Eval DB for the banded explorer book

One engine evaluation per position of the merged explorer book
(`E:\chess\position-stats\explorer_banded_2013_2026`), built by `explorer-extract evals`
from the two Lichess evaluation datasets. The spec is the blog repo's `docs/eval-db-spec.md`.
`_build.meta.json` records the exact inputs (book meta digest, dataset revisions and file sha256s,
the exe's sha256), parameters, parse-failure counts and timings.

## Files

| File | What |
|---|---|
| `bkt<iii>.parquet` | The rows whose `position_hash` is in bucket `iii` (`((h % 512) + 512) % 512`), sorted by (`position_hash`, `epd`), zstd, 1Mi-row groups. |
| `_manifest.parquet` | One row per bucket: rows, parents, children, cloud/fishnet counts, ambiguous and ep-variant rows, book coverage, bytes, sha256 of the file. |
| `_ambiguous.parquet` | Every child-only hash that carries two or more eval EPDs (see Caveats). |
| `_coverage.parquet` | By ply: book parent positions, those with an eval, and `SUM(total)` over each (game-weighted). |
| `_build.meta.json` | Provenance. |
| `_DONE` | Written last. Absent means the DB is incomplete. |

## Read contract

- **By position:** `h = zobrist_int64(board)`, `epd = board.epd()` (python-chess, legal en passant),
  read `bkt{bucket(h):03d}.parquet` and filter `position_hash = h AND epd = epd`.
- The key is (`position_hash`, `epd`). Collision twins (two EPDs under one hash) are separate rows,
  exactly as in the book. Hash-only consumers must exclude or handle the book's `_collisions` hashes
  and the rows with `hash_ambiguous`; never take the first row of a hash.
- `pq.read_table(p)` works (no hive columns here), as do DuckDB and Polars.

## Columns

| Column | Type | Meaning |
|---|---|---|
| `position_hash` | int64 | The book's polyglot hash (`EnPassantMode::PseudoLegal`). |
| `epd` | utf8 | The book's EPD (4 fields, ep square only if the capture is legal). |
| `in_book` | utf8 | `parent`: (`position_hash`, `epd`) is a book (`parent_hash`, `parent_epd`). `child`: `position_hash` is a book `child_hash` and not a book `parent_hash`. `parent` wins. |
| `source` | utf8 | `cloud` if the cloud dataset evaluates the position, else `fishnet`. |
| `cp`, `mate` | int32 | The chosen eval, White's point of view. Exactly one is non-null. `mate` > 0: White mates; `mate` 0: the side to move is mated. |
| `eval_cp` | int16 | The chosen eval on the old DB's scale: cp clamped to ±2000, mate → sign·2000, mate 0 by the mated side. |
| `cloud_depth`, `cloud_knodes`, `cloud_cp`, `cloud_mate`, `cloud_line` | | The chosen cloud row; `cloud_line` is its whole PV (UCI), whose first move is the best move. |
| `cloud_n_evals` | int32 | Distinct (depth, knodes) among the position's cloud rows. |
| `fishnet_cp`, `fishnet_mate` | int32 | The fishnet lower median in the best tier present. |
| `fishnet_tier` | utf8 | `nnue` (2021-01 on), `classical` (2016-01..2020-12) or `early` (2013–2015). |
| `fishnet_n_tier`, `fishnet_n` | int32 | Fishnet rows in that tier, and in all tiers. |
| `hash_ambiguous` | bool | A child-only hash with two or more eval EPDs. |
| `fishnet_disagrees` | bool | The old DB's `fishnet-decisive` condition, **not applied**: source cloud, the fishnet median saturated at ±2000 with `fishnet_n_tier` ≥ 5, and `sign(fishnet) != sign(eval_cp)` (so a cloud 0 counts). |

## Choice rules

- **Cloud** (`Lichess/chess-position-evaluations`, one row per PV): the row with the max `depth`, then
  the max `knodes`, then the first in file order (file name, row index). A position's evals are
  stored as blocks of PVs, best first, so that is the deepest eval's first PV (Lichess's own
  recommendation).
- **Fishnet** (`Lichess/fishnet-evals`, one row per position occurrence per analysed game, no depth):
  only the newest tier present counts; within it, the lower median of the observed scores ordered
  White-POV (mate > 0 above every cp, a shorter mate higher; mate < 0 below every cp, a shorter mate
  lower). The lower median is always an observed value. The `move` column (the human's move) is ignored.

## Position identity

Every source FEN is parsed with shakmaty (standard castling, strict validation); rows that fail
(parse errors, illegal positions, Chess960 castling, missing scores) are skipped and counted by
reason in `_build.meta.json`. The EPD and hash are computed with the same functions the book's month
mode used. A position that could carry an en-passant square whose capture is pseudo-legal but illegal
(a pinned capturer) has a book hash that includes the ep file while its EPD omits it; the eval is
emitted under each such hash as well (the "ep-variant" rows, counted in `_manifest`). The EPD is the
same, so a variant is always the same position.

## Caveats

- **Child-only matches are by hash alone.** The book stores no child EPD, so a child-only row is a
  hash match. A random 64-bit collision between a foreign position and a book child cannot be told
  apart; a few dozen are expected at this scale. Where the eval side has two or more EPDs under one
  child hash, every one is kept and flagged `hash_ambiguous` (listed in `_ambiguous.parquet`).
- A child hash that is also a book parent hash matches only as a parent (by EPD): its other EPDs are
  not emitted as children.
- Fishnet mate scores before 2016 are unreliable (dataset card); they are kept, in tier `early`, and
  only used when no newer tier exists.
- `fishnet_n_tier` and `fishnet_n` saturate at 2^31 − 1.
