# Self-contained extraction source inventories

Authorized November 25. Work in the maintained takeover worktree; do not edit
running pool producers or historical frozen releases.

- [x] Create a versioned inventory from the independent six-speed provenance,
  verifying current source filenames and footer counts before accepting it.
- [x] Store it beside the source dataset and bind a copy inside each extraction.
- [x] Update maintained launchers and preserve legacy archive receipts/settings.
- [x] Test missing/extra files, changed inventory, missing speeds, interrupted
  packaging, archive-only resume and legacy compatibility.
- [x] Publish the real metadata package, attach it to the completed extraction,
  freeze a new launcher release, and document its safe use and verification.

Inventory file sizes/mtime/row counts are not full source-byte checksums. Retain
the independent historical evidence; never silently regenerate expected inputs
from a potentially incomplete source tree. Existing dataset payloads, completion
markers and their accepted hashes must remain unchanged.

## Completed migration

The source inventory is now owned by the original games dataset:

`D:/data/chess/standard-chess-games-compressed/_source_metadata/inventory-v1.json`

Matching bound copies are in:

- `F:/chess/explorer_brc_p51_pilot/_source_metadata/`
- `D:/chess/explorer_brc_p51_pilot_stage/_source_metadata/`

Each directory includes `inventory-v1.binding.json`, containing the file SHA-256
and canonical content identity. Publication writes the binding last. A partial
copy cannot be used until reconciled against an independently valid package.

The package covers **163 months, 4,179 source partitions and 7,153,458,738 games**
across all six speeds, January 2013–July 2026. Its size is 1,874,541 bytes. It embeds
the historical six-speed evidence in full, including the independently recorded
file list and game count. Current source paths are relative to the source root.

- Inventory file SHA-256: `710d06201b676ad725cf81289fa49191eb25c5643322e5087c6e033e68efc2b5`.
- Canonical identity: `86fd273ae515379543aad63d4e878b1ce41d8c4dcead1aeccf113977716c05bf`.
- Frozen standalone launcher: `D:/chess/bin/source-inventory-launcher-20261125-v1`.
- Its `release.json` pins all three Python modules and the original extractor.

**659 existing metadata files were byte-for-byte unchanged**, including current
archive receipts, completion markers, build provenance, staged source inventories,
root settings, and accepted merged-book metadata. No payload was rewritten, deleted
or rebuilt. No running producer or historical frozen release was changed.

## Ownership and resume behavior

Source identity belongs beside the source dataset. Each extraction retains its own
copy so recovery does not depend on a different derived dataset. The extraction's
existing `_provenance`, `_archive_receipts`, settings and completion markers continue
to describe its own build. The published merged book retains its own metadata.

The maintained launcher resolves an explicit `--source-manifest` first; otherwise
it uses a complete embedded archive/stage copy, then the source-owned package. It
rejects conflicting embedded copies, unsupported months/speeds, changed source
filenames or metadata, and missing bindings. Historically absent speeds remain
valid only when the independent evidence supports their absence.

The legacy `--source-provenance` option remains an explicit import path. Production
defaults no longer point at `F:/chess/explorer_banded/rust_p30k/_provenance`.
Maintained migration and pilot wrappers now pass `--source-manifest`.

For a future resume, use the frozen `run_month_extraction.py` above with the original
source/stage/archive paths and settings. In particular, retain the extractor path
`D:/chess/bin/ply51-pilot-20261115/explorer-extract.exe` (SHA-256
`c4b03572818a713c3e40cb85b80f88e518641f8fd9584e070e91a372a5463e74`). Existing settings
bind both executable path and hash; copying it to a new path is not an equivalent
resume. Keep `--max-ply 51 --key128 --term-slices --events Blitz Classical Rapid
--verify hash`. The source-manifest argument may be omitted to use the embedded copy.
Do not rerun the historical migration coordinator merely to adopt this metadata.

For a new input generation, create a separately named package using independent
evidence and `source_inventory.py create --output ...`, then select it explicitly
for a new output root. Never overwrite an existing bound package or retrofit a
changed source generation into an accepted extraction. Current identity includes
mtime, so a source copy must preserve timestamps to pass unchanged; altered
timestamps require deliberate validation, not bypassing the comparison.

## Verification

- Eight source-inventory unit tests passed.
- Three extraction/archive integration tests passed using the pinned Rust extractor.
  These exercise missing inputs, output corruption, interrupted publication, and
  resume after removing both the old reference and source-owned package, using the
  extraction's embedded copy without re-extracting the completed month.
- The deployed frozen launcher rechecked all 163 BRC source months (file lists,
  footer counts, sizes and mtimes), reconciled saved stage/archive inventories,
  checked all three package identities and verified release/extractor hashes.
  A guard rejected any attempted JSON read under the old `rust_p30k` directory;
  there were zero such reads.
- This was metadata validation, not a new full-byte audit of the multi-TB payloads.
  Existing extraction and archive verification receipts remain intact.
- Main and mini pool status remained running and advanced during the migration.

Reproduction tests (set `PYTHONPATH=python` and `EXPLORER_EXTRACT_EXE` to the pinned
extractor; use the project's Python environment):

```text
python -B -m unittest discover -s python/tests -p test_source_inventory.py -v
python -B -m unittest _test_month_archive -v
```

Evidence is in `docs/evidence/source-inventory-migration-20261125/`: deployment and
verification receipts, before/after metadata hashes, scripts, and test logs.

The old six-speed archive still has dashboard consumers and non-BRC populations
absent from the new archive. Removing this provenance dependency does **not** make
its entire payload redundant or authorize deletion.
