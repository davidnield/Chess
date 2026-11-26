param([switch]$Execute)
$ErrorActionPreference = 'Stop'
$target = 'H:\chess\chessdb\chess-20260702\data'
$evidence = 'D:\chess\acceptance\chessdb-archive-query-20261125'
$release = 'D:\chess\bin\chessdb-archive-query-20261125-v1'
$accepted = Get-Content -LiteralPath (Join-Path $evidence 'frozen-acceptance.json') -Raw | ConvertFrom-Json
if (-not $accepted.verified -or -not $accepted.native_exact -or $accepted.rows -ne 971) { throw 'Missing accepted parity' }
$binding = Get-Content -LiteralPath (Join-Path $release 'binding.json') -Raw | ConvertFrom-Json
foreach ($name in @('executable','index')) {
    if ((Get-FileHash -LiteralPath $binding.$name -Algorithm SHA256).Hash.ToLowerInvariant() -ne $binding."${name}_sha256") { throw "Changed $name" }
}
$marker = 'F:\chess\chessdb-archive-20260702-v6-zstd3\ARCHIVE.COMPLETE.json'
if ((Get-FileHash -LiteralPath $marker -Algorithm SHA256).Hash.ToLowerInvariant() -ne $binding.marker_sha256) { throw 'Changed archive' }
$resolved = (Resolve-Path -LiteralPath $target).ProviderPath
if ($resolved -cne $target -or (Get-Item -LiteralPath $resolved).Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Unsafe source root' }
$files = @(Get-ChildItem -LiteralPath $resolved -Force)
if ($files.Count -ne 582 -or @($files | Where-Object { $_.PSIsContainer -or ($_.Attributes -band [IO.FileAttributes]::ReparsePoint) }).Count) { throw 'Unexpected source contents' }
$bytes = ($files | Measure-Object -Property Length -Sum).Sum
if ($bytes -ne 1188168027771) { throw "Unexpected source bytes $bytes" }
$before = (Get-PSDrive H).Free
$inventory = [ordered]@{target=$resolved;files=$files.Count;bytes=$bytes;free_before=$before;utc=[DateTime]::UtcNow.ToString('o');entries=@($files | Select-Object Name,Length);archive_marker_sha256=$binding.marker_sha256}
if (-not $Execute) { $inventory | ConvertTo-Json -Depth 4; exit 0 }
$receipt = Join-Path $evidence 'windows-retirement.json'
if (Test-Path -LiteralPath $receipt) { throw 'Retirement already recorded' }
$zip = Join-Path $evidence 'original-source-metadata.zip'
if (Test-Path -LiteralPath $zip) { throw 'Provenance archive already exists; review before retry' }
Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [IO.Compression.ZipFile]::Open($zip, [IO.Compression.ZipArchiveMode]::Create)
try {
    foreach ($file in $files | Where-Object { $_.Extension -ne '.sst' }) {
        if ($file.Length -gt 128MB) { throw 'Unexpected large non-SST metadata' }
        [IO.Compression.ZipFileExtensions]::CreateEntryFromFile($archive, $file.FullName, $file.Name) | Out-Null
    }
    [IO.Compression.ZipFileExtensions]::CreateEntryFromFile($archive, 'H:\chess\chessdb\manifest.tsv', 'download-manifest.tsv') | Out-Null
} finally { $archive.Dispose() }
$inventory | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $evidence 'windows-retirement-preflight.json') -Encoding utf8
# Exactly the previously resolved, regular payload directory; never the parent.
Remove-Item -LiteralPath $resolved -Recurse -Force
if (Test-Path -LiteralPath $resolved) { throw 'Source directory still exists' }
if (-not (Test-Path -LiteralPath 'H:\chess\chessdb\manifest.tsv')) { throw 'Parent provenance missing' }
$inventory.removed = $true
$inventory.free_after = (Get-PSDrive H).Free
$inventory.metadata_zip_sha256 = (Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash.ToLowerInvariant()
$inventory | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $receipt -Encoding utf8
[pscustomobject]$inventory | Select-Object target,files,bytes,free_before,free_after,removed | ConvertTo-Json
