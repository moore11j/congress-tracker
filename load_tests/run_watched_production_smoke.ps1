param(
  [ValidateSet("smoke", "staged")]
  [string]$Mode = "staged",

  [ValidateSet("small", "prod50", "prod75", "prod200", "prod300", "prod400", "backend_api_400", "apphost_api_400", "apphost_pages_400", "apphost_pages_500", "medium", "large", "target")]
  [string]$TestProfile = "small",

  [ValidateRange(1, 10)]
  [int]$SmokeVus = 3,

  [string]$SmokeDuration = "90s",
  [string]$BaseUrl = "https://app.walnutmarkets.com",
  [string]$ApiBaseUrl = "https://app.walnutmarkets.com",
  [string]$FlyApp = "congress-tracker-api",
  [string]$ContainerName = "walnut-k6-smoke",
  [int]$SlowClusterThreshold = 5,
  [int]$SlowClusterWindowSeconds = 30,
  [switch]$ApproveProduction,
  [switch]$DryRun,
  [switch]$PreflightOnly
)

$ErrorActionPreference = "Stop"

$ProductionHosts = @(
  "app.walnutmarkets.com",
  "walnutmarkets.com",
  "congress-tracker-api.fly.dev"
)

$CoreSlowPattern = "db_pool_checkout_slow.*path=/api/(events|tickers/[^ ]+/(context-bundle|signals-summary|government-contracts)|institutions/|market/quotes)"
$HardStopPattern = "db_pool_timeout|heavy_route_saturated|OperationalError|status=50[03]| 500 | 503 "
$InstitutionalHardStopPattern = "institutional_latest_job_start|institutional_latest_job_invalid|institutional_latest_job_stale_lock_recovered|ingest_institutional|scheduled[-_]latest"

function Normalize-ProcessPathEnvironment {
  $envVars = [Environment]::GetEnvironmentVariables("Process")
  $pathValue = $null

  foreach ($entry in $envVars.GetEnumerator()) {
    if ($entry.Key.ToString().ToLowerInvariant() -eq "path" -and $entry.Value) {
      $pathValue = [string]$entry.Value
      break
    }
  }

  if (-not $pathValue) {
    return
  }

  foreach ($entry in @($envVars.GetEnumerator() | Where-Object { $_.Key.ToString().ToLowerInvariant() -eq "path" })) {
    [Environment]::SetEnvironmentVariable([string]$entry.Key, $null, "Process")
  }

  [Environment]::SetEnvironmentVariable("Path", $pathValue, "Process")
}

Normalize-ProcessPathEnvironment

function Test-ProductionTarget {
  param([string]$Url)
  $lower = $Url.ToLowerInvariant()
  foreach ($hostName in $ProductionHosts) {
    if ($lower.Contains($hostName)) {
      return $true
    }
  }
  return $false
}

function Strip-Ansi {
  param([string]$Value)
  return ($Value -replace "`e\[[0-9;]*[A-Za-z]", "")
}

function Try-ParseLogUtc {
  param([string]$Line)
  $clean = Strip-Ansi $Line
  if ($clean -match "(?<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)") {
    return [datetime]::Parse(
      $Matches.ts,
      [Globalization.CultureInfo]::InvariantCulture,
      [Globalization.DateTimeStyles]::AssumeUniversal -bor [Globalization.DateTimeStyles]::AdjustToUniversal
    )
  }
  return $null
}

function Get-HardStopMatch {
  param([string]$Line)

  if ($Line -match $HardStopPattern) {
    return $Matches[0]
  }

  if ($Line -match "institutional_latest_job_disabled.*reason=(env_disabled|durable_or_env_disabled)") {
    return $null
  }
  if ($Line -match "institutional_latest_job_skipped") {
    return $null
  }
  if ($Line -match 'job succeeded.*run_institutional_latest_job\.sh') {
    return $null
  }
  if ($Line -match 'msg=starting iteration=.*run_institutional_latest_job\.sh') {
    return $null
  }
  if ($Line -match $InstitutionalHardStopPattern) {
    return $Matches[0]
  }

  return $null
}

function Join-NativeArguments {
  param([string[]]$Arguments)

  $quoted = foreach ($arg in $Arguments) {
    if ($null -eq $arg) {
      '""'
    } elseif ($arg -match '[\s"]') {
      '"' + ($arg -replace '(\\*)"', '$1$1\"' -replace '(\\+)$', '$1$1') + '"'
    } else {
      $arg
    }
  }
  return ($quoted -join " ")
}

function Invoke-CapturedNativeCommand {
  param(
    [string]$FilePath,
    [string[]]$Arguments,
    [switch]$AllowFailure
  )

  $processInfo = New-Object System.Diagnostics.ProcessStartInfo
  $processInfo.FileName = $FilePath
  $processInfo.Arguments = Join-NativeArguments $Arguments
  $processInfo.UseShellExecute = $false
  $processInfo.RedirectStandardOutput = $true
  $processInfo.RedirectStandardError = $true
  $processInfo.CreateNoWindow = $true

  $process = New-Object System.Diagnostics.Process
  $process.StartInfo = $processInfo

  try {
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    $combined = (($stdout, $stderr) | Where-Object { $_ -and $_.Length -gt 0 }) -join "`n"

    $result = [pscustomobject]@{
      ExitCode = $process.ExitCode
      Stdout = $stdout
      Stderr = $stderr
      Output = $combined
      CombinedOutput = $combined
    }

    if ($result.ExitCode -ne 0 -and -not $AllowFailure) {
      throw "$FilePath $($Arguments -join ' ') failed with exit code $($result.ExitCode)`n$($result.CombinedOutput)"
    }

    return $result
  } finally {
    $process.Dispose()
  }
}

function Start-NativeCommandToFiles {
  param(
    [string]$FilePath,
    [string[]]$Arguments,
    [string]$StdoutPath,
    [string]$StderrPath
  )

  $process = Start-Process `
    -FilePath $FilePath `
    -ArgumentList (Join-NativeArguments $Arguments) `
    -RedirectStandardOutput $StdoutPath `
    -RedirectStandardError $StderrPath `
    -NoNewWindow `
    -PassThru

  [pscustomobject]@{
    Process = $process
    StdoutWriter = $null
    StderrWriter = $null
  }
}

function Ensure-Directory {
  param([string]$Path)
  if (-not (Test-Path $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function Initialize-ResultFile {
  param([string]$Path)
  $parent = Split-Path -Parent $Path
  Ensure-Directory $parent
  Set-Content -Path $Path -Value "" -Encoding UTF8
}

function Save-DockerLogs {
  param(
    [string]$Name,
    [string]$StdoutPath,
    [string]$StderrPath,
    [string]$CombinedPath
  )

  $result = Invoke-CapturedNativeCommand "docker" @("logs", $Name) -AllowFailure
  Set-Content -Path $StdoutPath -Value $result.Stdout -Encoding UTF8 -NoNewline
  Set-Content -Path $StderrPath -Value $result.Stderr -Encoding UTF8 -NoNewline
  Set-Content -Path $CombinedPath -Value $result.Output -Encoding UTF8

  return $result
}

function Get-DockerContainerState {
  param([string]$Name)

  $result = Invoke-CapturedNativeCommand "docker" @("inspect", "--format", "{{.State.Running}} {{.State.ExitCode}}", $Name) -AllowFailure
  if ($result.ExitCode -ne 0 -or -not $result.Stdout.Trim()) {
    return $null
  }

  $parts = $result.Stdout.Trim() -split "\s+"
  [pscustomobject]@{
    Running = $parts[0] -eq "true"
    ExitCode = [int]$parts[1]
  }
}

function Start-DetachedDockerContainer {
  param([string[]]$Arguments)
  $result = Invoke-CapturedNativeCommand "docker" $Arguments
  return $result.Stdout.Trim()
}

function Stop-CapturedProcess {
  param([object]$CapturedProcess)
  if (-not $CapturedProcess) {
    return
  }

  $process = $CapturedProcess.Process
  if ($process -and -not $process.HasExited) {
    try {
      $process.Kill()
    } catch {
      Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
  }
}

function Dispose-CapturedProcess {
  param([object]$CapturedProcess)
  if (-not $CapturedProcess) {
    return
  }

  if ($CapturedProcess.Process) {
    $CapturedProcess.Process.Dispose()
  }
  if ($CapturedProcess.StdoutWriter) {
    $CapturedProcess.StdoutWriter.Dispose()
  }
  if ($CapturedProcess.StderrWriter) {
    $CapturedProcess.StderrWriter.Dispose()
  }
}

function Run-RouteProbes {
  $routes = @(
    @{ name = "/health"; url = "https://congress-tracker-api.fly.dev/health"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") },
    @{ name = "/api/events?limit=5&enrich_prices=0"; url = "$ApiBaseUrl/api/events?limit=5&enrich_prices=0"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") },
    @{ name = "/api/events?limit=10&enrich_prices=1"; url = "$ApiBaseUrl/api/events?limit=10&enrich_prices=1"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") },
    @{ name = "/api/events?limit=50&enrich_prices=1"; url = "$ApiBaseUrl/api/events?limit=50&enrich_prices=1"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") },
    @{ name = "/api/market/quotes"; url = "$ApiBaseUrl/api/market/quotes?symbols=NVDA,AAPL,LMT,PLTR"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") },
    @{ name = "/api/tickers/AAPL/context-bundle"; url = "$ApiBaseUrl/api/tickers/AAPL/context-bundle"; headers = @("Accept: application/json", "User-Agent: Mozilla/5.0 Walnut-k6-watch/1.0", "Referer: $BaseUrl/ticker/AAPL", "X-Walnut-Request-Source: client", "X-Walnut-Panel: context-bundle", "X-Walnut-Route-Family: ticker", "X-Walnut-Active-User: browser") },
    @{ name = "/api/tickers/AAPL/signals-summary"; url = "$ApiBaseUrl/api/tickers/AAPL/signals-summary"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") },
    @{ name = "/api/tickers/NVDA/government-contracts"; url = "$ApiBaseUrl/api/tickers/NVDA/government-contracts"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") },
    @{ name = "/api/institutions/0001067983"; url = "$ApiBaseUrl/api/institutions/0001067983"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") }
  )

  foreach ($item in $routes) {
    $args = @("-sS", "-o", "NUL", "-w", "%{http_code} %{time_total} %{size_download}", "--max-time", "30")
    foreach ($header in $item.headers) {
      $args += @("-H", $header)
    }
    $args += $item.url
    $result = Invoke-CapturedNativeCommand "curl.exe" $args
    $out = $result.Stdout.Trim()
    $parts = $out -split " "
    [pscustomobject]@{
      Route = $item.name
      Status = $parts[0]
      Ms = [math]::Round(([double]$parts[1]) * 1000, 1)
      Bytes = $parts[2]
    }
  }
}

function Confirm-SchedulerDisabled {
  $envResult = Invoke-CapturedNativeCommand "flyctl" @("ssh", "console", "-a", $FlyApp, "--pty=false", "-C", "printenv INSTITUTIONAL_SCHEDULED_INGEST_ENABLED") -AllowFailure
  $lockCommand = 'sh -lc ''for p in /tmp/institutional_latest_job.lock /data/institutional_latest_job.lock /tmp/institutional_ingest.lock /data/institutional_ingest.lock; do if [ -e "$p" ]; then ls -ld "$p"; exit 1; fi; done; echo no institutional lock files'''
  $lockResult = Invoke-CapturedNativeCommand "flyctl" @("ssh", "console", "-a", $FlyApp, "--pty=false", "-C", $lockCommand) -AllowFailure

  $isDisabled = $envResult.Output -match "(?m)^false\s*$"
  $hasNoLocks = $lockResult.Output -match "no institutional lock files"
  $envExitOk = $envResult.ExitCode -eq 0 -or ($isDisabled -and $envResult.Output -match "The handle is invalid")
  $lockExitOk = $lockResult.ExitCode -eq 0 -or ($hasNoLocks -and $lockResult.Output -match "The handle is invalid")

  if (-not $envExitOk) {
    throw "Unable to confirm scheduler env state. flyctl exit_code=$($envResult.ExitCode)`n$($envResult.Output)"
  }

  if (-not $lockExitOk) {
    throw "Unable to confirm institutional lock state. flyctl exit_code=$($lockResult.ExitCode)`n$($lockResult.Output)"
  }

  [pscustomobject]@{
    EnvDisabled = $isDisabled
    EnvOutput = $envResult.Output.Trim()
    NoLockFiles = $hasNoLocks
    LockOutput = $lockResult.Output.Trim()
  }
}

function Assert-NoRunningK6Container {
  $existing = Invoke-CapturedNativeCommand "docker" @("ps", "-a", "--filter", "ancestor=grafana/k6", "--format", "{{.ID}} {{.Names}} {{.Status}}") -AllowFailure
  $matching = @($existing.Output -split "`n" | Where-Object { $_.Trim().Length -gt 0 })
  if ($matching.Count -gt 0 -and ($matching -join "").Trim().Length -gt 0) {
    throw "A grafana/k6 container is still running:`n$($matching -join "`n")"
  }
}

function Stop-K6Container {
  Invoke-CapturedNativeCommand "docker" @("stop", $ContainerName) -AllowFailure | Out-Null
}

$isProduction = (Test-ProductionTarget $BaseUrl) -or (Test-ProductionTarget $ApiBaseUrl)
if ($isProduction -and -not $ApproveProduction) {
  throw "Production target requires -ApproveProduction and the k6 ALLOW_PRODUCTION_LOAD_TEST=true guard."
}

$k6Script = if ($Mode -eq "staged") { "load_tests/k6/walnut_capacity_stages.js" } else { "load_tests/k6/walnut_capacity_smoke.js" }
$dockerArgs = @(
  "run", "-d",
  "--name", $ContainerName,
  "-v", "${PWD}:/work",
  "-w", "/work",
  "-e", "ALLOW_PRODUCTION_LOAD_TEST=true",
  "-e", "BASE_URL=$BaseUrl",
  "-e", "API_BASE_URL=$ApiBaseUrl"
)

if ($Mode -eq "staged") {
  $dockerArgs += @("-e", "TEST_PROFILE=$TestProfile")
} else {
  $dockerArgs += @("-e", "SMOKE_VUS=$SmokeVus", "-e", "SMOKE_DURATION=$SmokeDuration")
}

$k6EnvArgs = @(
  "-e", "ALLOW_PRODUCTION_LOAD_TEST=true",
  "-e", "BASE_URL=$BaseUrl",
  "-e", "API_BASE_URL=$ApiBaseUrl"
)

if ($Mode -eq "staged") {
  $k6EnvArgs += @("-e", "TEST_PROFILE=$TestProfile")
} else {
  $k6EnvArgs += @("-e", "SMOKE_VUS=$SmokeVus", "-e", "SMOKE_DURATION=$SmokeDuration")
}

$dockerArgs += @("grafana/k6", "run") + $k6EnvArgs + @($k6Script)

if ($DryRun) {
  Write-Host "Dry run only. No production load, Fly log stream, scheduler probe, or Docker container was started."
  Write-Host "Container name: $ContainerName"
  Write-Host "Mode: $Mode"
  Write-Host "Docker command:"
  Write-Host "docker $($dockerArgs -join ' ')"
  Write-Host "Log filtering: records are ignored until their Fly log timestamp is >= test_start_utc."
  exit 0
}

Assert-NoRunningK6Container

Write-Host "Preflight: scheduler and lock state"
$schedulerBefore = Confirm-SchedulerDisabled
$schedulerBefore | Format-List
if (-not $schedulerBefore.EnvDisabled -or -not $schedulerBefore.NoLockFiles) {
  throw "Scheduler preflight failed; refusing to run k6."
}

Write-Host "Preflight: core route probes"
$preRoutes = @(Run-RouteProbes)
$preRoutes | Format-Table -AutoSize
if (@($preRoutes | Where-Object { $_.Status -ne "200" }).Count -gt 0) {
  throw "Core route preflight failed; refusing to run k6."
}

if ($PreflightOnly) {
  Write-Host "Preflight only. Docker/k6 was not started."
  exit 0
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$resultsDir = Join-Path $scriptDir "results"
$resultBaseName = "walnut-k6-$Mode-$TestProfile-$stamp"
$logOut = Join-Path $resultsDir "$resultBaseName.fly.out.log"
$logErr = Join-Path $resultsDir "$resultBaseName.fly.err.log"
$k6Out = Join-Path $resultsDir "$resultBaseName.k6.out.log"
$k6Err = Join-Path $resultsDir "$resultBaseName.k6.err.log"
$k6Combined = Join-Path $resultsDir "$resultBaseName.k6.combined.log"

Ensure-Directory $resultsDir
Initialize-ResultFile $logOut
Initialize-ResultFile $logErr
Initialize-ResultFile $k6Out
Initialize-ResultFile $k6Err
Initialize-ResultFile $k6Combined

$fly = $null
$k6ContainerId = $null
$k6ExitCode = 1
$stopReason = $null
$lineOffset = 0
$currentRecordUtc = $null
$slowEvents = New-Object System.Collections.Generic.List[datetime]
$lastK6ConsoleUpdateUtc = [datetime]::MinValue

try {
  $testStartUtc = [datetime]::UtcNow
  Write-Host "test_start_utc=$($testStartUtc.ToString("o"))"

  $fly = Start-NativeCommandToFiles "flyctl" @("logs", "-a", $FlyApp) $logOut $logErr
  Start-Sleep -Seconds 2

  $k6ContainerId = Start-DetachedDockerContainer $dockerArgs
  Write-Host "k6_container=$k6ContainerId"

  while ($true) {
    Start-Sleep -Seconds 5
    Save-DockerLogs $ContainerName $k6Out $k6Err $k6Combined | Out-Null

    $nowUtc = [datetime]::UtcNow
    if (($nowUtc - $lastK6ConsoleUpdateUtc).TotalSeconds -ge 60) {
      $lastK6ConsoleUpdateUtc = $nowUtc
      Write-Host "k6 progress tail ($($nowUtc.ToString("o"))):"
      if (Test-Path $k6Out) {
        Get-Content -Path $k6Out -Tail 5 | ForEach-Object { Write-Host $_ }
      }
      if (Test-Path $k6Err) {
        $stderrTail = @(Get-Content -Path $k6Err -Tail 3 | Where-Object { $_.Trim().Length -gt 0 })
        if ($stderrTail.Count -gt 0) {
          Write-Host "k6 stderr tail:"
          $stderrTail | ForEach-Object { Write-Host $_ }
        }
      }
    }

    $state = Get-DockerContainerState $ContainerName
    if (-not $state) {
      throw "Unable to inspect k6 container $ContainerName. Logs so far: $k6Combined"
    }

    if (-not $state.Running) {
      $k6ExitCode = $state.ExitCode
      break
    }

    if (-not (Test-Path $logOut)) {
      continue
    }

    $lines = @(Get-Content $logOut)
    for ($i = $lineOffset; $i -lt $lines.Count; $i++) {
      $raw = $lines[$i]
      $line = Strip-Ansi $raw
      $lineUtc = Try-ParseLogUtc $line
      if ($lineUtc) {
        $currentRecordUtc = $lineUtc
      }

      if (-not $currentRecordUtc) {
        continue
      }

      if ($currentRecordUtc -lt $testStartUtc) {
        continue
      }

      $hardStopMatch = Get-HardStopMatch $line
      if ($hardStopMatch) {
        $stopReason = "in-test hard-stop log pattern at $($currentRecordUtc.ToString("o")): $hardStopMatch"
        break
      }

      if ($line -match $CoreSlowPattern) {
        $slowEvents.Add($currentRecordUtc)
        $windowStart = $currentRecordUtc.AddSeconds(-1 * $SlowClusterWindowSeconds)
        $recentSlowEvents = New-Object System.Collections.Generic.List[datetime]
        foreach ($eventUtc in $slowEvents) {
          if ($eventUtc -ge $windowStart) {
            $recentSlowEvents.Add($eventUtc)
          }
        }
        $slowEvents = $recentSlowEvents
        if ($slowEvents.Count -ge $SlowClusterThreshold) {
          $stopReason = "in-test db_pool_checkout_slow cluster on core routes: $($slowEvents.Count) matches in ${SlowClusterWindowSeconds}s ending $($currentRecordUtc.ToString("o"))"
          break
        }
      }
    }
    $lineOffset = $lines.Count

    if ($stopReason) {
      Write-Warning $stopReason
      Stop-K6Container
      break
    }
  }

  if ($stopReason) {
    do {
      Start-Sleep -Seconds 2
      $state = Get-DockerContainerState $ContainerName
    } while ($state -and $state.Running)

    if ($state) {
      $k6ExitCode = $state.ExitCode
    }
  }
} finally {
  $testEndUtc = [datetime]::UtcNow
  Write-Host "test_end_utc=$($testEndUtc.ToString("o"))"

  if ($fly) {
    Stop-CapturedProcess $fly
  }

  $finalState = Get-DockerContainerState $ContainerName
  if ($finalState -and $finalState.Running) {
    Write-Warning "Wrapper exiting while k6 is still running; stopping $ContainerName."
    Stop-K6Container
  }

  if ($finalState) {
    Save-DockerLogs $ContainerName $k6Out $k6Err $k6Combined | Out-Null
    Invoke-CapturedNativeCommand "docker" @("rm", "-f", $ContainerName) -AllowFailure | Out-Null
  }

  Write-Host "Post-test: verify no k6 container remains"
  Invoke-CapturedNativeCommand "docker" @("ps", "-a", "--filter", "ancestor=grafana/k6", "--format", "table {{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Names}}") -AllowFailure | Select-Object -ExpandProperty Output
  Assert-NoRunningK6Container

  Write-Host "Post-test: scheduler and lock state"
  Confirm-SchedulerDisabled | Format-List

  Write-Host "Post-test: core route probes"
  Run-RouteProbes | Format-Table -AutoSize

  Write-Host "Output files:"
  Write-Host "k6 stdout: $k6Out"
  Write-Host "k6 stderr: $k6Err"
  Write-Host "k6 combined: $k6Combined"
  Write-Host "Fly stdout: $logOut"
  Write-Host "Fly stderr: $logErr"

  Dispose-CapturedProcess $fly
}

if ($stopReason) {
  Write-Error $stopReason
  exit 2
}

if ($k6ContainerId) {
  exit $k6ExitCode
}

exit 1
