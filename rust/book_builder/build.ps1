# Build the extension and install it as python/book_builder_rs.pyd (gitignored).
#   pwsh rust/book_builder/build.ps1            release build + install
#   pwsh rust/book_builder/build.ps1 -Test      also cargo test and the Python differential test
param([switch]$Test)
$ErrorActionPreference = "Stop"
$crate = $PSScriptRoot
$repo = (Resolve-Path "$crate\..\..").Path
$env:RUSTUP_TOOLCHAIN = "stable"
$env:CARGO_TARGET_DIR = "D:\rust-target\book_builder"     # C: is nearly full
$env:PYO3_PYTHON = "$repo\.venv\Scripts\python.exe"
if (-not (Test-Path $env:PYO3_PYTHON)) {                   # a worktree: the main checkout's venv
    $env:PYO3_PYTHON = "C:\Users\David\Documents\Chess\.venv\Scripts\python.exe"
}
Push-Location $crate
try {
    cargo build --release --locked --features extension-module
    if ($LASTEXITCODE) { throw "cargo build failed" }
    Copy-Item "$env:CARGO_TARGET_DIR\release\book_builder_rs.dll" "$repo\python\book_builder_rs.pyd" -Force
    Write-Host "installed $repo\python\book_builder_rs.pyd"
    if ($Test) {
        # cargo test links libpython: put the interpreter's directory (python3.dll) on PATH
        $base = & $env:PYO3_PYTHON -c "import sys; print(sys.base_prefix)"
        $env:PATH = "$base;$env:PATH"
        cargo test --release --locked
        if ($LASTEXITCODE) { throw "cargo test failed" }
        & $env:PYO3_PYTHON "$repo\python\_test_book_builder_rs.py"
        if ($LASTEXITCODE) { throw "differential test failed" }
    }
} finally {
    Pop-Location
}
