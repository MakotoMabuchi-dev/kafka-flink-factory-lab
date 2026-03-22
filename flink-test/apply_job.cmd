@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "BASE_DIR=%SCRIPT_DIR%.."

if "%~1"=="" (
  set "SQL_FILE=%SCRIPT_DIR%sql\job.sql"
) else (
  set "SQL_FILE=%~1"
)

where py >nul 2>nul
if %errorlevel%==0 (
  py -3 "%BASE_DIR%\scripts\lab_cli.py" apply-sql --file "%SQL_FILE%"
) else (
  python "%BASE_DIR%\scripts\lab_cli.py" apply-sql --file "%SQL_FILE%"
)
