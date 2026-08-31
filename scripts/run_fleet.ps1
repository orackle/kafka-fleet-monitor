$projectRoot = (Get-Item "$PSScriptRoot\..").FullName
$nodes = @("web-01", "web-02", "db-01", "cache-01", "worker-01")

foreach ($node in $nodes) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$projectRoot'; & '$projectRoot\venv\Scripts\python.exe' '$projectRoot\producers\node_agent.py' '$node'"
}