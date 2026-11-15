# Run the three approved months after the existing builders and scorers release the machine.
# Run from a pinned copy alongside run_month_extraction.py, month_archive.py, and explorer-extract.exe.
[CmdletBinding()]
param(
    [string]$Python = 'C:\Users\David\Documents\Chess\.venv\Scripts\python.exe',
    [string]$Source = 'D:\data\chess\standard-chess-games-compressed',
    [string]$Reference = 'F:\chess\explorer_banded\rust_p30k\_provenance',
    [string]$Stage = 'D:\chess\explorer_brc_p51_pilot_stage',
    [string]$Archive = 'F:\chess\explorer_brc_p51_pilot',
    [int]$Threads = 12,
    [int]$MemGB = 55
)
$ErrorActionPreference = 'Stop'
$pin = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot 'pin.json') | ConvertFrom-Json
function Confirm-Pin {
    foreach ($entry in $pin.files.PSObject.Properties) {
        $digest = (Get-FileHash -LiteralPath (Join-Path $PSScriptRoot $entry.Name) -Algorithm SHA256).Hash
        if ($digest -ne $entry.Value) { throw "Pinned file changed: $($entry.Name)" }
    }
}
Confirm-Pin
$exe = Join-Path $PSScriptRoot 'explorer-extract.exe'
$launcher = Join-Path $PSScriptRoot 'run_month_extraction.py'
function Get-BusyReason {
    $jobs = @(Get-CimInstance Win32_Process -Filter "Name='cmd.exe' OR Name='python.exe' OR Name='explorer-extract.exe' OR Name LIKE 'stockfish%.exe'" |
        Where-Object {
            $_.Name -like 'stockfish*.exe' -or $_.Name -eq 'explorer-extract.exe' -or
            $_.CommandLine -match 'lf_chain2?\.bat|score_lf[^ ]*\.(bat|py)|run_rare_book\.py|convertibility_eval\.py|build_budget_books\.py|rare_verify\.py'
        })
    if ($jobs.Count) { return "active builder, scorer or engine ($($jobs.Count) processes)" }
    $mem = Get-CimInstance Win32_PerfFormattedData_PerfOS_Memory
    if (($mem.CommitLimit - $mem.CommittedBytes) -lt (($MemGB + 8) * 1e9) -or $mem.AvailableBytes -lt ($MemGB * 1e9)) {
        return "waiting for extraction memory"
    }
    $cpu = Get-CimInstance Win32_PerfFormattedData_PerfOS_Processor -Filter "Name='_Total'"
    if ($cpu.PercentProcessorTime -gt 20) { return "machine busy ($($cpu.PercentProcessorTime)% CPU)" }
}
function Wait-Quiet {
    $quietChecks = 0
    $lastReason = ''
    while ($quietChecks -lt 2) {
        $reason = Get-BusyReason
        if ($reason) {
            $quietChecks = 0
            if ($reason -ne $lastReason) { Write-Output "$(Get-Date -Format o) $reason" }
        } else {
            $quietChecks++
        }
        $lastReason = $reason
        if ($quietChecks -lt 2) { Start-Sleep -Seconds 60 }
    }
}
# Isolate each timing from the next extraction and from overlapping archive I/O.
$months = @('2026_3','2024_6','2020_4')
$measurements = @()
foreach ($month in $months) {
    Wait-Quiet
    Confirm-Pin
    & $Python -u $launcher --exe $exe --source $Source --source-provenance $Reference --months $month --stage $Stage --dest $Archive --events Blitz Classical Rapid --max-ply 51 --key128 --term-slices --mem-gb $MemGB --threads $Threads --verify hash
    if ($LASTEXITCODE -ne 0) { throw "Month $month exited $LASTEXITCODE. The launcher can resume its stage." }
    $receipt = Get-Content -Raw -LiteralPath (Join-Path $Archive "_archive_receipts/$month.json") | ConvertFrom-Json
    $prov = Get-Content -Raw -LiteralPath (Join-Path $Archive "_provenance/month=$month.json") | ConvertFrom-Json
    $year, $number = $month.Split('_')
    $terminal = Get-Item -LiteralPath (Join-Path $Archive "_term/year=${year}_month=${number}.term.parquet")
    $measurements += [pscustomobject]@{
        Month=$month
        SourceGames=$prov.games_in_footers
        KeptGames=$prov.counters.kept
        Plies=$prov.counters.plies
        Rows=$prov.rows
        TermRows=$prov.term_rows
        MoveParquetGB=$($prov.output_bytes / 1e9)
        TermParquetGB=$($terminal.Length / 1e9)
        Passes=$prov.passes
        PeakExtractorCommitGB=$($prov.peak_commit_bytes / 1e9)
        ExtractSeconds=$prov.seconds
        ArchiveGB=$($receipt.bytes / 1e9)
        StreamWriteSeconds=$receipt.stream_write_seconds
        ReadbackSeconds=$receipt.readback_seconds
        VerifiedMBPerSecond=$($receipt.bytes / 1e6 / ($receipt.stream_write_seconds + $receipt.readback_seconds))
    }
    $report = Join-Path $PSScriptRoot 'pilot-measurements.json'
    ConvertTo-Json -InputObject $measurements | Set-Content -LiteralPath "$report.tmp"
    Move-Item -LiteralPath "$report.tmp" -Destination $report -Force
}
Write-Output "$(Get-Date -Format o) Three pilot months verified. Measurements saved to $report."

