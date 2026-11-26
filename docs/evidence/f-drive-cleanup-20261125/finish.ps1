# Resume the prepared cleanup after a transient nonempty-directory response.
$ErrorActionPreference = 'Stop'
$before = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'before.json') -Raw | ConvertFrom-Json
$allowed = @(
 'F:\chess\position-stats\_pooled_partials_ge1800_2013_2026_brc',
 'F:\chess\chessdb-archive-benchmark-20261122-v1',
 'F:\chess\chessdb-archive-tests-20261122-v1',
 'F:\chess\chessdb-archive-tests-20261122-v4',
 'F:\chess\chessdb-archive-tests-20261122-v5',
 'F:\chess\chessdb-speed-benchmark-20261123-smoke',
 'F:\chess\chessdb-speed-benchmark-20261123-v1',
 'F:\chess\chessdb-speed-benchmark-20261123-v3',
 'F:\chess\chessdb-speed-benchmark-20261123-v4',
 'F:\$RECYCLE.BIN\$R3F8UI2', 'F:\$RECYCLE.BIN\$I3F8UI2'
)
if ($before.targets.Count -ne $allowed.Count) { throw 'Unexpected prepared scope' }
foreach ($record in $before.targets) {
 if ($allowed -cnotcontains $record.path) { throw 'Unexpected target' }
 foreach ($saved in $record.preserved) {
  if ((Get-FileHash -LiteralPath $saved.copy).Hash -ne $saved.sha256) { throw 'Saved metadata changed' }
 }
}
foreach ($record in $before.targets) {
 if (Test-Path -LiteralPath $record.path) {
  $resolved = (Resolve-Path -LiteralPath $record.path).ProviderPath.TrimEnd('\')
  if ($resolved -cne $record.path -or $allowed -cnotcontains $resolved) { throw 'Resolved target changed' }
  $item = Get-Item -LiteralPath $resolved -Force
  $items = @($item)
  if ($item.PSIsContainer) { $items += @(Get-ChildItem -LiteralPath $resolved -Force -Recurse) }
  if (@($items | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint }).Count) { throw 'Unexpected reparse point' }
  Write-Output "Deleting $resolved"
  # Remove files first, then empty directories deepest-first. Windows' recursive
  # remover returned nonempty-directory errors despite successful child removal.
  foreach ($file in @($items | Where-Object { -not $_.PSIsContainer })) {
   Remove-Item -LiteralPath $file.FullName -Force
  }
  foreach ($dir in @($items | Where-Object PSIsContainer | Sort-Object { $_.FullName.Length } -Descending)) {
   if (@(Get-ChildItem -LiteralPath $dir.FullName -Force).Count) { throw "Directory still contains files: $($dir.FullName)" }
   Remove-Item -LiteralPath $dir.FullName -Force
  }
 }
 if (Test-Path -LiteralPath $record.path) { throw 'Deletion incomplete' }
 @{utc=[DateTime]::UtcNow.ToString('o');path=$record.path;deleted=$true} | ConvertTo-Json -Compress | Add-Content -LiteralPath (Join-Path $PSScriptRoot 'deleted.jsonl') -Encoding UTF8
}
$protected = @('F:\chess\chessdb-archive-20260702-v6-zstd3','F:\chess\explorer_brc_p51_pilot','F:\chess\explorer_banded\rust_p30k')
foreach ($path in $protected) { if (-not (Test-Path -LiteralPath $path -PathType Container)) { throw 'Protected path missing' } }
if (@(Get-ChildItem -LiteralPath 'F:\$RECYCLE.BIN' -Force | Where-Object Name -ne 'desktop.ini').Count) { throw 'Recycle bin not empty' }
$volume = Get-Volume -DriveLetter F
$result = @{utc=[DateTime]::UtcNow.ToString('o');deleted_file_bytes=($before.targets | Measure-Object bytes -Sum).Sum;reclaimed_bytes=$volume.SizeRemaining-$before.free_bytes;free_bytes=$volume.SizeRemaining;deleted_targets=$before.targets.Count;recycle_empty=$true;protected_paths_present=$protected;health=$volume.HealthStatus}
$result | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $PSScriptRoot 'after.json') -Encoding UTF8
$result | ConvertTo-Json -Depth 4
