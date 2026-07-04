param(
  [ValidateSet("smoke", "staged")]
  [string]$Mode = "staged",

  [ValidateSet("small", "medium", "large", "target")]
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
$HardStopPattern = "db_pool_timeout|heavy_route_saturated|OperationalError|status=50[03]| 500 | 503 |institutional_latest_job|ingest_institutional|institutional scheduled"

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

  $stdoutPath = [IO.Path]::GetTempFileName()
  $stderrPath = [IO.Path]::GetTempFileName()
  try {
    $process = Start-Process `
      -FilePath $FilePath `
      -ArgumentList (Join-NativeArguments $Arguments) `
      -RedirectStandardOutput $stdoutPath `
      -RedirectStandardError $stderrPath `
      -NoNewWindow `
      -Wait `
      -PassThru

    $stdout = if (Test-Path $stdoutPath) { Get-Content -Raw $stdoutPath } else { "" }
    $stderr = if (Test-Path $stderrPath) { Get-Content -Raw $stderrPath } else { "" }
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
    Remove-Item -LiteralPath $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
  }
}

function Run-RouteProbes {
  $routes = @(
    @{ name = "/health"; url = "https://congress-tracker-api.fly.dev/health"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") },
    @{ name = "/api/events?limit=5&enrich_prices=0"; url = "$ApiBaseUrl/api/events?limit=5&enrich_prices=0"; headers = @("User-Agent: Walnut-k6-watch/1.0", "X-Walnut-Monitor-Probe: 1") },
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
  $existing = Invoke-CapturedNativeCommand "docker" @("ps", "--filter", "ancestor=grafana/k6", "--format", "{{.ID}} {{.Names}} {{.Status}}") -AllowFailure
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
  "run", "--rm", "-i",
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

$dockerArgs += @("grafana/k6", "run", $k6Script)

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
$logOut = Join-Path $env:TEMP "walnut-k6-fly-$stamp.out.log"
$logErr = Join-Path $env:TEMP "walnut-k6-fly-$stamp.err.log"
$k6Out = Join-Path $env:TEMP "walnut-k6-$stamp.out.log"
$k6Err = Join-Path $env:TEMP "walnut-k6-$stamp.err.log"

$fly = $null
$k6 = $null
$stopReason = $null
$lineOffset = 0
$currentRecordUtc = $null
$slowEvents = New-Object System.Collections.Generic.List[datetime]

try {
  $testStartUtc = [datetime]::UtcNow
  Write-Host "test_start_utc=$($testStartUtc.ToString("o"))"

  $fly = Start-Process -FilePath "flyctl" -ArgumentList @("logs", "-a", $FlyApp) -RedirectStandardOutput $logOut -RedirectStandardError $logErr -PassThru -WindowStyle Hidden
  Start-Sleep -Seconds 2

  $k6 = Start-Process -FilePath "docker" -ArgumentList $dockerArgs -RedirectStandardOutput $k6Out -RedirectStandardError $k6Err -PassThru -WindowStyle Hidden

  while (-not $k6.HasExited) {
    Start-Sleep -Seconds 5
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

      if ($line -match $HardStopPattern) {
        $stopReason = "in-test hard-stop log pattern at $($currentRecordUtc.ToString("o")): $($Matches[0])"
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

  if ($k6) {
    $k6.WaitForExit()
  }
} finally {
  $testEndUtc = [datetime]::UtcNow
  Write-Host "test_end_utc=$($testEndUtc.ToString("o"))"

  if ($fly -and -not $fly.HasExited) {
    Stop-Process -Id $fly.Id -Force -ErrorAction SilentlyContinue
  }

  Write-Host "Post-test: verify no k6 container remains"
  Invoke-CapturedNativeCommand "docker" @("ps", "--filter", "ancestor=grafana/k6", "--format", "table {{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Names}}") -AllowFailure | Select-Object -ExpandProperty Output
  Assert-NoRunningK6Container

  Write-Host "Post-test: scheduler and lock state"
  Confirm-SchedulerDisabled | Format-List

  Write-Host "Post-test: core route probes"
  Run-RouteProbes | Format-Table -AutoSize

  Write-Host "Output files:"
  Write-Host "k6 stdout: $k6Out"
  Write-Host "k6 stderr: $k6Err"
  Write-Host "Fly stdout: $logOut"
  Write-Host "Fly stderr: $logErr"
}

if ($stopReason) {
  Write-Error $stopReason
  exit 2
}

if ($k6) {
  exit $k6.ExitCode
}

exit 1
