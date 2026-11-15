@echo off
"C:\Users\David\Documents\Chess\.venv\Scripts\python.exe" -B -X utf8 -u "%~dp0resume_ply51_trip.py" %*
set "tripStatus=%ERRORLEVEL%"
if not "%tripStatus%"=="0" pause
exit /b %tripStatus%
