@echo off
setlocal

where py >nul 2>nul
if %errorlevel%==0 (
  py -3 "%~dp0scripts\lab_cli.py" watch-topic %*
) else (
  python "%~dp0scripts\lab_cli.py" watch-topic %*
)
