param(
    [int]$DurationSeconds = 30,
    [int]$KillIntervalSeconds = 5
)

$ErrorActionPreference = "Continue"
Remove-Item *.state.json -ErrorAction SilentlyContinue

$nodeDefs = @(
    @{ Id = "node1"; Addr = "9001"; Peers = "node2=127.0.0.1:9002,node3=127.0.0.1:9003" },
    @{ Id = "node2"; Addr = "9002"; Peers = "node1=127.0.0.1:9001,node3=127.0.0.1:9003" },
    @{ Id = "node3"; Addr = "9003"; Peers = "node1=127.0.0.1:9001,node2=127.0.0.1:9002" }
)

$procs = @{}
foreach ($n in $nodeDefs) {
    $p = Start-Process -FilePath ".\bin\miniraft.exe" `
        -ArgumentList "-id=$($n.Id)", "-addr=:$($n.Addr)", "-peers=$($n.Peers)" `
        -RedirectStandardOutput "$($n.Id).log" -RedirectStandardError "$($n.Id).err.log" `
        -PassThru -NoNewWindow
    $procs[$n.Id] = $p
}

Write-Host "Started 3 nodes. Waiting 3s for initial election..."
Start-Sleep -Seconds 3

$written = @{}
$writeCounter = 0
$deadline = (Get-Date).AddSeconds($DurationSeconds)
$nextKill = (Get-Date).AddSeconds($KillIntervalSeconds)
$failures = 0
$maxVerifyPerIteration = 5

while ((Get-Date) -lt $deadline) {

    # --- Write a new key ---
    $writeCounter++
    $key = "k$writeCounter"
    $val = "v$writeCounter"   # renamed from $value to avoid any ambiguity
    $putOutputRaw = & .\bin\client.exe -addr="127.0.0.1:9001" -op=put -key=$key -value=$val 2>&1
    $putLast = ($putOutputRaw | Select-Object -Last 1).ToString().Trim()

    if ($putLast -match "committed") {
        $written[$key] = $val
        Write-Host "PUT $key=$val -> OK"
    } else {
        Write-Host "PUT $key=$val -> FAILED: $putLast"
    }

    # --- Verify a bounded random sample of previously-written keys ---
    $allKeys = @($written.Keys)
    $sampleSize = [Math]::Min($maxVerifyPerIteration, $allKeys.Count)
    $sample = $allKeys | Get-Random -Count $sampleSize

    foreach ($k in $sample) {
        $expected = $written[$k]
        $getOutputRaw = & .\bin\client.exe -addr="127.0.0.1:9001" -op=get -key=$k 2>&1
        $getLast = ($getOutputRaw | Select-Object -Last 1).ToString().Trim()
        if ($getLast -ne $expected) {
            Write-Host "!!! DATA LOSS/MISMATCH: key=$k expected=$expected got=$getLast"
            $failures++
        }
    }

    # --- Periodically kill a random live node ---
    if ((Get-Date) -ge $nextKill) {
        $aliveIds = $procs.Keys | Where-Object { -not $procs[$_].HasExited }
        if ($aliveIds.Count -gt 0) {
            $victim = $aliveIds | Get-Random
            Write-Host ">>> Killing $victim <<<"
            Stop-Process -Id $procs[$victim].Id -Force
        }

        foreach ($n in $nodeDefs) {
            if ($procs[$n.Id].HasExited) {
                $p = Start-Process -FilePath ".\bin\miniraft.exe" `
                    -ArgumentList "-id=$($n.Id)", "-addr=:$($n.Addr)", "-peers=$($n.Peers)" `
                    -RedirectStandardOutput "$($n.Id).log" -RedirectStandardError "$($n.Id).err.log" `
                    -PassThru -NoNewWindow
                $procs[$n.Id] = $p
                Write-Host ">>> Restarted $($n.Id) <<<"
            }
        }
        $nextKill = (Get-Date).AddSeconds($KillIntervalSeconds)
    }

    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "=== Chaos test complete ==="
Write-Host "Writes attempted: $writeCounter, keys tracked: $($written.Count), failures: $failures"

foreach ($p in $procs.Values) {
    if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force }
}

if ($failures -gt 0) {
    Write-Host "RESULT: FAIL - data loss or mismatch detected"
    exit 1
} else {
    Write-Host "RESULT: PASS - all committed writes survived"
    exit 0
}