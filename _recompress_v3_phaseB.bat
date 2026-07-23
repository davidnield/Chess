@echo off
rem Phase B restart: recipe-v3 recompression for 2021+ at 3 workers (was 2).
rem Phase A (2013-2020) already complete. Resumable: partitions with a _SUCCESS
rem sentinel are skipped, so this picks up at 2022-03/02/01 and all of 2021.
rem Memory headroom confirmed low this week -> 3 workers is safe.
cd /d C:\Users\David\Documents\Chess
set PY=.venv\Scripts\python.exe
set SRC=F:/chess/standard-chess-games/data
set OUT=E:/chess/standard-chess-games-compressed-v3
set LOG=logs\recompress_v3.log

echo ================================================================ >> %LOG%
echo === recompress v3 Phase B RESTART (3 workers) %date% %time% === >> %LOG%

%PY% python\process_pgn_parquets.py --source %SRC% --output %OUT% --start-year 2021 --workers 3 >> %LOG% 2>&1
set RCB=%ERRORLEVEL%

%PY% -c "import sys; sys.path.insert(0, 'python'); import notify; notify.send('Recompress v3 Phase B finished (rc=%RCB%)', 'Phase B restart (2021+, 3 workers) exit=%RCB%. Grep logs/recompress_v3.log for FAILED before comparing against D:.')" >> %LOG% 2>&1

echo === recompress v3 Phase B done rc=%RCB% %date% %time% === >> %LOG%
