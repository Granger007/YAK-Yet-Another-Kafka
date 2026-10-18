@echo off
:: kill-restart.bat  -  Kill or restart node N.
:: Usage: scripts\kill-restart.bat kill    2
::        scripts\kill-restart.bat restart 2
set ACTION=%1
set NODE_ID=%2
set /a PORT=8000+%NODE_ID%

if "%ACTION%"=="kill" goto :kill_node
if "%ACTION%"=="restart" goto :restart_node
echo Usage: kill-restart.bat {kill^|restart} ^<node_id^>
exit /b 1

:kill_node
for /f "tokens=5" %%P in ('"netstat -aon | findstr :%PORT% | findstr LISTENING"') do (
    echo Killing node %NODE_ID% with PID %%P
    taskkill /PID %%P /F
)
goto :eof

:restart_node
call :kill_node
timeout /t 1 /nobreak >nul
if not exist data\node-%NODE_ID% mkdir data\node-%NODE_ID%
start /b cmd /c "python -m broker.broker --id %NODE_ID% --port %PORT% --peers 1:localhost:8001,2:localhost:8002,3:localhost:8003,4:localhost:8004,5:localhost:8005 --data-dir data\node-%NODE_ID% --log-level INFO > logs\node-%NODE_ID%.log 2>&1"
echo Node %NODE_ID% restarted.
goto :eof
