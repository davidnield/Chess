# F: filesystem-warning investigation — November 25, 2026

David requested an investigation after Windows reported F: as **Warning / Full
Repair Needed**. The evidence currently points to a persistent dirty flag. No
filesystem corruption or archive checksum discrepancy was found in the checks
described below. This is not proof that every sector or every file on F: is sound.

## Findings

F: is the 16 TB Seagate Expansion USB HDD, Disk 4, formatted exFAT. Free space is
1,011,630,997,504 bytes (942.2 GiB). `fsutil dirty query F:` confirms **Dirty**.
The physical disk reports **Healthy / Online**. The sandbox's zero-free-space and
access-denied observations were not reliable drive-health evidence; unsandboxed
diagnostics were required. During CHKDSK, ordinary file access was also denied
and some volume queries had no filesystem details. Access returned when it exited.

System log review for the last 21 days found Disk 4 **surprise removal**, event
157, on **November 22 at 15:25:22 PST**, and an older retried I/O at block zero
on November 6. No new disk/USB errors were found during this investigation.
Disk numbers alone are not immutable hardware identities; this is the disk number
currently assigned to F:. The removal is a plausible cause of the dirty flag,
not a proven explanation of the specific disconnect (cable, power, unplugging,
driver, etc.). No failing component has been identified.

## Completed read-only checks

- **`chkdsk.exe F:`**, with no repair/mark-clean/dismount switches, ran from
  approximately 18:42:49 to 18:56:46 PST. Exit code **0**. Final result:
  **“Windows has scanned the file system and found no problems.”** It reported
  176,067 files, 168,212 indexes, 262,144-byte allocation units and 0 KB in bad
  sectors. This was a filesystem check, **not** a full surface scan of 16 TB;
  that bad-sector line does not prove every physical sector is healthy.
- The scan initially printed **Access is denied**, but proceeded to the complete
  successful filesystem summary. No repair was requested. F:'s dirty flag and
  warning remained afterward, as expected for a check without `/markclean`.
- **All 3,345 archive files** still match the accepted inventory's sizes and
  timestamps; all 3,345 receipt hashes and the archive completion-marker hash
  match. This is metadata/receipt coverage, not a new hash of all 2.09 TB.
- **Eight whole archive shards**, spread through the published inventory, totaling
  **4,865,486,272 bytes**, match their recorded SHA-256 checksums.
- The frozen Rust reader reproduced **all 971 accepted reference queries exactly**.
  Inventory, sampled hashes and replay took 93.2 seconds together.
- David approved a UAC prompt for a **read-only hardware diagnostic**. Windows
  PowerShell 5.1's default script-file policy blocked the first helper before it
  produced results. The same approved diagnostic ran successfully under the
  installed PowerShell 7 host, with an administrator token and no policy change.
  Windows reported **46°C**, **22,199 power-on hours**, **0 total read errors** and
  **0 uncorrected read errors**. Write-error fields were unavailable. The separate
  full SMART prediction-status query returned **Not supported**. These counters
  are useful but do not establish a complete SMART/physical-media diagnosis.

The prior complete archive byte/decode audit finished **November 24 at 23:45
PST**, after the November 22 removal event. The 971-query acceptance also passed
at 18:08 today, before this investigation. Those earlier checks and the fresh
ones are reassuring; none justifies calling an unchecked sector healthy.

## Recommendation and approval boundary

The next proportionate action is an administrator **`chkdsk F: /markclean`** pass.
Microsoft documents `/markclean` for FAT/FAT32/exFAT: it marks the volume clean
only if no corruption is detected. It should repeat the check and clear the stale
flag if the result remains clean. It is **not executed or scheduled** by this
investigation; David approved read-only diagnostics, not a drive-state change.
Expect another scan on the order of 15 minutes based on the observed run, with
F: temporarily unavailable to other readers. Do not substitute `/f`, `/r`, `/x`,
a forced dismount, or a raw boot-sector edit.

A reviewable, identity-guarded helper is preserved at
`docs/evidence/f-drive-investigation-20261125/mark-clean-conditional.ps1` in the
maintained worktree. It checks the exact volume/disk and accepted archive marker, runs only
the conditional mark-clean command, and records the exit code, dirty state and
volume status. Obtain David's separate approval before executing it. Afterward,
verify the warning clears and replay the fixed archive queries. If errors appear
or the dirty flag quickly returns, stop and investigate USB/power/hardware and
backup/recovery options before repair. Do not interpret recurring problems as
merely a cosmetic flag.

The existing pool jobs use D:/E:/H: and continued throughout; no active release,
dataset, drive policy or filesystem flag was changed. Keep the archive STOP and
the prohibition on automatic F: repair. The deleted raw copies remain retired;
do not recreate them as part of this diagnostic. A second verified archive copy
is still desirable for resilience, independently of this warning; no large copy
was launched or storage reservation changed.

## Evidence and references

Local evidence: `docs/evidence/f-drive-investigation-20261125/`, including
`chkdsk-readonly-initial.log`, `initial-status.json`, `disk-removal.xml`,
`archive-check.json`, `query-results.json`, and `elevated-hardware-health.json`.
`archive-check-during-scan.json` records a temporary permission failure while
CHKDSK was active; it is superseded by the successful post-scan result, not a
checksum failure. The archived probe script reads the pinned production index,
hashes sampled files, and compares the fixed query panel; it never writes to F:.

- [Microsoft: CHKDSK options and limitations](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/chkdsk)
- [Microsoft: dirty-bit causes](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/fsutil-dirty)
- [Microsoft: exFAT VolumeDirty and MediaFailure fields](https://learn.microsoft.com/en-us/windows/win32/fileio/exfat-specification)

The dirty flag means possible inconsistency, not an automatic diagnosis of bad
hardware. A clean filesystem check plus passing sample hashes supports the stale
flag explanation, with the uncertainty and remaining checks described above.
