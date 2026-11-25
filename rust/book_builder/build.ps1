# Build the extension and install it as python/book_builder_rs.pyd (gitignored).
#   pwsh rust/book_builder/build.ps1            release build + install
#   pwsh rust/book_builder/build.ps1 -Test      also cargo test and the Python differential test
param([switch]$Test, [switch]$Optimizer, [string]$TargetDir = "D:\rust-target\book_builder", [int]$Jobs = 2)
$ErrorActionPreference = "Stop"
$crate = $PSScriptRoot
$repo = (Resolve-Path "$crate\..\..").Path
$env:RUSTUP_TOOLCHAIN = "stable"
$env:CARGO_TARGET_DIR = $TargetDir
$env:CARGO_BUILD_JOBS = "$Jobs"
$env:PYO3_PYTHON = "$repo\.venv\Scripts\python.exe"
if (-not (Test-Path $env:PYO3_PYTHON)) {                   # a worktree: the main checkout's venv
    $env:PYO3_PYTHON = "C:\Users\David\Documents\Chess\.venv\Scripts\python.exe"
}
Push-Location $crate
try {
    $features = if ($Optimizer) { "extension-module,optimizer" } else { "extension-module" }
    cargo build --release --locked --lib --features $features
    if ($LASTEXITCODE) { throw "cargo build failed" }
    Copy-Item "$env:CARGO_TARGET_DIR\release\book_builder_rs.dll" "$repo\python\book_builder_rs.pyd" -Force
    Write-Host "installed $repo\python\book_builder_rs.pyd"
    if ($Test) {
        # cargo test links libpython: put the interpreter's directory (python3.dll) on PATH
        $base = & $env:PYO3_PYTHON -c "import sys; print(sys.base_prefix)"
        $env:PATH = "$base;$env:PATH"
        if ($Optimizer) { cargo test --release --locked --lib --features optimizer }
        else { cargo test --release --locked --lib }
        if ($LASTEXITCODE) { throw "cargo test failed" }
        & $env:PYO3_PYTHON "$repo\python\_test_book_builder_rs.py"
        if ($LASTEXITCODE) { throw "differential test failed" }
        if ($Optimizer) {
            & $env:PYO3_PYTHON "$repo\python\_test_optimizer_interface.py"
            if ($LASTEXITCODE) { throw "optimizer interface test failed" }
        }
    }
} finally {
    Pop-Location
}
