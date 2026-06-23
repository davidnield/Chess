@echo off
cd /d "C:\Users\David\Documents\Chess"
echo === Finding Python/uv on PATH ===
echo.
echo --- where uv ---
where uv 2>nul || echo uv not found on PATH
echo.
echo --- where python ---
where python 2>nul || echo python not found on PATH
echo.
echo --- where python3 ---
where python3 2>nul || echo python3 not found on PATH
echo.
echo --- Checking common Python 3.11 locations ---
if exist "C:\Users\David\AppData\Local\Programs\Python\Python311\python.exe" (
    echo FOUND: C:\Users\David\AppData\Local\Programs\Python\Python311\python.exe
) else echo Not at AppData\Local\Programs\Python\Python311
if exist "C:\Python311\python.exe" (
    echo FOUND: C:\Python311\python.exe
) else echo Not at C:\Python311
echo.
echo --- .venv pyvenv.cfg ---
type .venv\pyvenv.cfg
echo.
echo --- Anaconda ---
if exist "C:\Users\David\anaconda3\python.exe" (
    echo FOUND Anaconda: C:\Users\David\anaconda3\python.exe
) else echo Not at C:\Users\David\anaconda3
if exist "C:\ProgramData\anaconda3\python.exe" (
    echo FOUND Anaconda: C:\ProgramData\anaconda3\python.exe
) else echo Not at C:\ProgramData\anaconda3
echo.
echo --- uv common locations ---
if exist "C:\Users\David\.local\bin\uv.exe" echo FOUND: C:\Users\David\.local\bin\uv.exe
if exist "C:\Users\David\AppData\Local\uv\bin\uv.exe" echo FOUND: C:\Users\David\AppData\Local\uv\bin\uv.exe
if exist "C:\Users\David\AppData\Roaming\uv\bin\uv.exe" echo FOUND: C:\Users\David\AppData\Roaming\uv\bin\uv.exe
echo.
pause
