@echo off
cd /d "C:\Users\David\Documents\Chess"
echo Starting Vacation Crunch Pipeline...
echo Working directory: %CD%
echo.

echo Step 1: uv python dir (showing where uv stores Python)...
"C:\Users\David\.local\bin\uv.exe" python dir
echo.

echo Step 2: Installing Python 3.11.15 via uv...
"C:\Users\David\.local\bin\uv.exe" python install 3.11.15
echo uv install exit code: %ERRORLEVEL%
echo.

echo Step 3: uv python list (confirming what is installed)...
"C:\Users\David\.local\bin\uv.exe" python list
echo.

echo Step 4: Running pipeline...
.venv\Scripts\python.exe python\run_2025_combined.py
echo.
echo Pipeline exited with code %ERRORLEVEL%
pause
