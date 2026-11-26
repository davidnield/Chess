# PREPARED ONLY. Requires David's separate approval before running.
# The only disk-changing action is CHKDSK /markclean: mark clean if its scan finds no corruption.
# No /f, /r, /x, forced dismount, direct boot-sector edit, or automatic repair.
$ErrorActionPreference = 'Stop'
$out = 'C:\Users\David\Documents\Chess\scratch\worktrees\sweep-preparation\docs\evidence\f-drive-investigation-20261125'
if (Test-Path -LiteralPath (Join-Path $out 'mark-clean-result.json')) { throw 'Prior result exists; review before rerunning' }
$admin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $admin) { throw 'Administrator token required' }
$partition = Get-Partition -DriveLetter F
$disk = $partition | Get-Disk
$volume = Get-Volume -DriveLetter F
if ($disk.Number -ne 4 -or $disk.FriendlyName -ne 'Seagate Expansion HDD' -or $volume.FileSystemType -ne 'exFAT' -or $volume.UniqueId -ne '\\?\Volume{7c94039a-746a-4cf3-afe7-38675c616008}\') { throw 'F drive identity differs' }
$check = Get-Content -LiteralPath (Join-Path $out 'archive-check.json') -Raw | ConvertFrom-Json
if (-not $check.passed -or $check.inventory_files -ne 3345 -or $check.query_count -ne 971) { throw 'Archive diagnostic acceptance missing' }
$marker = 'F:\chess\chessdb-archive-20260702-v6-zstd3\ARCHIVE.COMPLETE.json'
if ((Get-FileHash -LiteralPath $marker -Algorithm SHA256).Hash.ToLowerInvariant() -ne $check.marker_sha256) { throw 'Archive generation changed' }
$started = Get-Date -Format o
& "$env:SystemRoot\System32\chkdsk.exe" 'F:' '/markclean' 2>&1 | Tee-Object -FilePath (Join-Path $out 'mark-clean.log')
$code = $LASTEXITCODE
$result = [ordered]@{started=$started;finished=(Get-Date -Format o);command='chkdsk.exe F: /markclean';exit_code=$code;dirty_query=(& fsutil.exe dirty query F: 2>&1 | Out-String);volume=(Get-Volume -DriveLetter F | Select-Object HealthStatus,OperationalStatus,SizeRemaining)}
$result | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $out 'mark-clean-result.json') -Encoding utf8
exit $code
