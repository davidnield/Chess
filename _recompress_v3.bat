@echo off
rem Full-archive recompression to recipe v3 (zstd-19 + M1 strip + 1M row groups).
rem Reads F: raw HF dump, writes a FRESH tree on E: for ratio/read comparison —
rem D: canonical is NOT touched. Resumable: partitions with a _SUCCESS sentinel
rem are skipped, so rerunning this .bat after any crash continues where it died.
rem Phase A: 2013-2020 (small months) at 3 workers; Phase B: 2021+ at 2 workers
rem (v3 per-event buffered writer peaks 30-40 GB RAM on big recent months).
cd /d C:\Users\David\Documents\Chess
set PY=.venv\Scripts\python.exe
set SRC=F:/chess/standard-chess-games/data
set OUT=E:/chess/standard-chess-games-compressed-v3
set LOG=logs\recompress_v3.log

echo ================================================================ >> %LOG%
echo === recompress v3 launch %date% %time% === >> %LOG%

%PY% python\process_pgn_parquets.py --source %SRC% --output %OUT% --start-year 2013 --end-year 2020 --workers 3 >> %LOG% 2>&1
set RCA=%ERRORLEVEL%

%PY% python\process_pgn_parquets.py --source %SRC% --output %OUT% --start-year 2021 --workers 2 >> %LOG% 2>&1
set RCB=%ERRORLEVEL%

%PY% -c "import sys; sys.path.insert(0, 'python'); import notify; notify.send('Recompress v3 finished (rcA=%RCA% rcB=%RCB%)', 'Full-archive recipe-v3 recompression to E:/chess/standard-chess-games-compressed-v3 finished. Phase A (2013-2020, 3 workers) exit=%RCA%; Phase B (2021+, 2 workers) exit=%RCB%. Grep logs/recompress_v3.log for FAILED before comparing against D:.')" >> %LOG% 2>&1

echo === recompress v3 done rcA=%RCA% rcB=%RCB% %date% %time% === >> %LOG%
