@echo off
:: stop-cluster.bat  -  Kill all broker processes on Windows.
for /f "tokens=5" %%P in ('"netstat -aon | findstr :800[12345] | findstr LISTENING"') do (
    echo Killing PID %%P
    taskkill /PID %%P /F 2>nul
)
echo Cluster stopped.
