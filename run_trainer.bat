@echo off
rem Opening Trainer launcher — point a Windows shortcut at this file.
rem Starts the local server and opens the browser. Data (FSRS history, pack,
rem imported games) lives in trainer_data\ next to this script.
cd /d %~dp0
set PYTHONPATH=%~dp0python
.venv\Scripts\python.exe -m trainer_app --data "%~dp0trainer_data" %*
