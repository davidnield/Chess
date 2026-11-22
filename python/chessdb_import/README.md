# ChessDB import sources

This is the maintained successor to the formerly gitignored scratch/chessdb_import scripts.
The original generation and its exactness comparisons are described in
docs/claude-response-to-chatgpt-review-2026-11-22.md. The original bundle is at
D:/chess/acceptance/chessdb-import-20261122.

## Current entry points

- Run the import scripts only from a new frozen release. Their dataset-specific defaults
  refer to the July 2026 ChessDB snapshot and the accepted ply-51 explorer.
- After all menu buckets exist, run python/chessdb_publication.py. It hashes both
  tables against every original bucket receipt and publishes _MENUS.COMPLETE.json.
  The old _DONE name is invalid on Windows because _done is a directory.
- Build the candidate evaluation lookup with python/chessdb_lookup_run.py.
  It owns an exclusive OS lock, starts one fresh process per bucket, verifies actual
  input content on resume, compares existing old-generation buckets, and publishes
  only after the exact complete bucket set has been verified.
- The old Python build() entry point delegates to the same verified coordinator.
  Production CLI use requires the complete menu publication receipt.

The published base lookup and lifetime books remain unchanged. The new lookup is
a default-off candidate until its evaluation and adoption decision.

## Provenance limitations

Claude changed files in the historical running scan pin. Version 1 of scan_menus.py
was not preserved. Its output path was compared exactly with the point-lookup path
on 34 buckets; version 2 adds duplicate resolution. Preserve this history instead
of attributing every historical bucket to the new maintained sources.

The historical native executables and reader source/revisions are preserved under
the original acceptance bundle's native directory. The existing importer defaults
still refer to those installed WSL binaries. A future extraction release must pin
their identities and deployment as well as the Python sources; do not use a changed
native build under an old release identity.

The one-off make_evidence_bundle.py is superseded by the immutable snapshot tool;
its historical source remains in the original acceptance bundle. Analyses and
their original inputs must travel with the acceptance evidence, not only the reports.
