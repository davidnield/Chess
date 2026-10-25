@echo off
rem Opening Trainer with phone/laptop access on the HOME network (trainer_app.security).
rem Other devices open the address printed below and enter the access key once.
rem Not reachable from the internet (the router does not forward to it); see the README.
cd /d %~dp0
set PYTHONPATH=%~dp0python
.venv\Scripts\python.exe -m trainer_app --data "%~dp0trainer_data" --remote %*
