# scripts/stop-cluster.ps1
# Stops all Raft broker processes on ports 8001-8005

8001..8005 | ForEach-Object {
    $port = $_
    $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    foreach ($conn in $conns) {
        $procId = $conn.OwningProcess
        if ($procId) {
            Write-Host "Stopping process PID $procId on port $port..."
            Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
        }
    }
}

Write-Host "Cluster stopped."
