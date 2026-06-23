@echo off
echo Deleting stale tier-1 files from position_stats_2024.slices...
echo.

set SLICES_DIR=E:\chess\position-stats\position_stats_2024.slices\_fragments

if not exist "%SLICES_DIR%" (
    echo Directory not found: %SLICES_DIR%
    echo Trying G: drive...
    set SLICES_DIR=G:\chess\position-stats\position_stats_2024.slices\_fragments
)

if not exist "%SLICES_DIR%" (
    echo ERROR: Slices directory not found on E: or G:
    pause
    exit /b 1
)

echo Scanning for _tier1_*.parquet files in: %SLICES_DIR%
echo.

set COUNT=0
for /r "%SLICES_DIR%" %%f in (_tier1_*.parquet) do (
    echo Deleting: %%f
    del "%%f"
    set /a COUNT+=1
)

echo.
echo Done. Deleted %COUNT% tier-1 file(s).
echo.
pause
