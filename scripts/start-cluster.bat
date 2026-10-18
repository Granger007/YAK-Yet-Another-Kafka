@echo off
:: start-cluster.bat  -  Launch all 5 Raft broker nodes on Windows.
:: Logs go to logs\node-<id>.log

cd /d "%~dp0\.."
if not exist logs  mkdir logs
if not exist pids  mkdir pids
if not exist data  mkdir data

set ELECTION_MIN=5000
set ELECTION_MAX=8000
set HEARTBEAT=300

set ALL_PEERS=1:localhost:8001,2:localhost:8002,3:localhost:8003,4:localhost:8004,5:localhost:8005

for %%I in (1 2 3 4 5) do (
    set /a PORT=8000+%%I
    call :start_node %%I
)

echo.
echo All 5 nodes started. Logs in logs\
echo Status: curl http://localhost:8001/status
echo Stop:   scripts\stop-cluster.bat
goto :eof

:start_node
set NODE_ID=%1
set PORT=800%NODE_ID%
echo Starting node %NODE_ID% on port %PORT%
if not exist data\node-%NODE_ID% mkdir data\node-%NODE_ID%
start /b cmd /c "python -m broker.broker --id %NODE_ID% --port %PORT% --peers 1:localhost:8001,2:localhost:8002,3:localhost:8003,4:localhost:8004,5:localhost:8005 --data-dir data\node-%NODE_ID% --election-min-ms %ELECTION_MIN% --election-max-ms %ELECTION_MAX% --heartbeat-ms %HEARTBEAT% --log-level INFO > logs\node-%NODE_ID%.log 2>&1"
goto :eof
