$ErrorActionPreference = 'Stop'
$evidence = $PSScriptRoot
$targets = @(
    'F:\chess\position-stats\_pooled_partials_ge1800_2013_2026_brc',
    'F:\chess\chessdb-archive-benchmark-20261122-v1',
    'F:\chess\chessdb-archive-tests-20261122-v1',
    'F:\chess\chessdb-archive-tests-20261122-v4',
    'F:\chess\chessdb-archive-tests-20261122-v5',
    'F:\chess\chessdb-speed-benchmark-20261123-smoke',
    'F:\chess\chessdb-speed-benchmark-20261123-v1',
    'F:\chess\chessdb-speed-benchmark-20261123-v3',
    'F:\chess\chessdb-speed-benchmark-20261123-v4'
)
$recycle = 'F:\$RECYCLE.BIN'
$protected = @('F:\chess\chessdb-archive-20260702-v6-zstd3', 'F:\chess\explorer_brc_p51_pilot', 'F:\chess\explorer_banded\rust_p30k')
if (Test-Path -LiteralPath (Join-Path $evidence 'before.json')) { throw 'Cleanup already prepared; inspect receipts before retrying' }
$beforeSpace = (Get-Volume -DriveLetter F).SizeRemaining
$records = @()
$allTargets = @($targets) + @(Get-ChildItem -LiteralPath $recycle -Force | Where-Object Name -ne 'desktop.ini' | ForEach-Object FullName)
foreach ($target in $allTargets) {
    $resolved = (Resolve-Path -LiteralPath $target).ProviderPath.TrimEnd('\')
    $isDataset = $targets -contains $target
    if ($resolved -cne $target -or $protected -contains $resolved) { throw "Unexpected target: $resolved" }
    if ($isDataset) {
        if (-not $resolved.StartsWith('F:\chess\', [StringComparison]::OrdinalIgnoreCase)) { throw 'Outside dataset scope' }
    } elseif ((Split-Path -Parent $resolved) -ne $recycle) { throw 'Outside recycle scope' }
    # Check every ancestor and descendant before any recursive deletion.
    $ancestor = Get-Item -LiteralPath $resolved -Force
    while ($null -ne $ancestor) {
        if ($ancestor.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Reparse point: $($ancestor.FullName)" }
        $ancestor = $ancestor.Parent
    }
    $rootItem = Get-Item -LiteralPath $resolved -Force
    $items = if ($rootItem.PSIsContainer) { @(Get-ChildItem -LiteralPath $resolved -Recurse -Force) } else { @($rootItem) }
    if (@($items | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint }).Count) { throw "Reparse descendant: $resolved" }
    $files = @($items | Where-Object { -not $_.PSIsContainer })
    $saved = @()
    if ($isDataset) {
        foreach ($file in $files) {
            if ($file.Length -le 16MB -and ($file.Extension -in @('.json','.jsonl','.md','.txt','.log','.csv') -or $file.Name -like '*.DONE')) {
                $relative = $file.FullName.Substring(3)
                $copy = Join-Path (Join-Path $evidence 'preserved') $relative
                New-Item -ItemType Directory -Path (Split-Path -Parent $copy) -Force | Out-Null
                Copy-Item -LiteralPath $file.FullName -Destination $copy
                $hash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash
                if ((Get-FileHash -LiteralPath $copy -Algorithm SHA256).Hash -ne $hash) { throw 'Metadata copy mismatch' }
                $saved += @{source=$file.FullName; copy=$copy; sha256=$hash}
            }
        }
    }
    $records += @{path=$resolved; files=$files.Count; bytes=($files | Measure-Object Length -Sum).Sum; preserved=$saved}
}
$processes = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|duckdb|explorer|chessdb' } | Select-Object ProcessId,Name,CommandLine)
foreach ($process in $processes) {
    foreach ($target in $targets) {
        if ($process.CommandLine -and $process.CommandLine.IndexOf($target,[StringComparison]::OrdinalIgnoreCase) -ge 0) { throw "Active process references $target" }
    }
}
@{utc=[DateTime]::UtcNow.ToString('o');free_bytes=$beforeSpace;targets=$records;processes=$processes} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $evidence 'before.json') -Encoding UTF8
foreach ($record in $records) {
    $resolved = (Resolve-Path -LiteralPath $record.path).ProviderPath.TrimEnd('\')
    if ($resolved -cne $record.path -or $allTargets -notcontains $resolved) { throw 'Deletion target changed' }
    Write-Output "Deleting $resolved ($($record.bytes) bytes)"
    Remove-Item -LiteralPath $resolved -Recurse -Force
    if (Test-Path -LiteralPath $resolved) { throw "Deletion incomplete: $resolved" }
    @{utc=[DateTime]::UtcNow.ToString('o');path=$resolved;deleted=$true} | ConvertTo-Json -Compress | Add-Content -LiteralPath (Join-Path $evidence 'deleted.jsonl') -Encoding UTF8
}
foreach ($path in $protected) { if (-not (Test-Path -LiteralPath $path -PathType Container)) { throw "Protected path missing: $path" } }
$remaining = @(Get-ChildItem -LiteralPath $recycle -Force | Where-Object Name -ne 'desktop.ini')
if ($remaining.Count) { throw 'Recycle bin is not empty' }
$afterSpace = (Get-Volume -DriveLetter F).SizeRemaining
$result = @{utc=[DateTime]::UtcNow.ToString('o'); deleted_file_bytes=($records | Measure-Object bytes -Sum).Sum; reclaimed_bytes=$afterSpace-$beforeSpace; free_bytes=$afterSpace; deleted_targets=$records.Count; recycle_empty=$true; protected_paths_present=$protected}
$result | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $evidence 'after.json') -Encoding UTF8
$result | ConvertTo-Json -Depth 4
