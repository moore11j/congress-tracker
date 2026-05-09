[CmdletBinding()]
param(
    [switch]$SkipFrontend,
    [switch]$SkipBackend,
    [switch]$SkipSecrets
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$BackendPython = Join-Path $RepoRoot "backend\.venv\Scripts\python.exe"
if (-not (Test-Path $BackendPython)) {
    $BackendPython = "python"
}

function Invoke-SecurityStep {
    param(
        [string]$Name,
        [scriptblock]$Command
    )

    Write-Host ""
    Write-Host "== $Name =="
    & $Command
}

function Test-PythonModule {
    param(
        [string]$PythonExe,
        [string]$ModuleName
    )

    & $PythonExe -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('$ModuleName') else 1)" *> $null
    return $LASTEXITCODE -eq 0
}

function Assert-LastExitCode {
    param([string]$CommandName)

    if ($LASTEXITCODE -ne 0) {
        throw "$CommandName failed with exit code $LASTEXITCODE"
    }
}

function Resolve-Gitleaks {
    $Command = Get-Command gitleaks -ErrorAction SilentlyContinue
    if ($Command) {
        return $Command.Source
    }

    $WingetPackages = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Packages"
    if (Test-Path $WingetPackages) {
        $Installed = Get-ChildItem -Path $WingetPackages -Recurse -Filter gitleaks.exe -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($Installed) {
            return $Installed.FullName
        }
    }

    return $null
}

Push-Location $RepoRoot
try {
    if (-not $SkipFrontend) {
        Invoke-SecurityStep "Frontend production dependency audit" {
            Push-Location (Join-Path $RepoRoot "frontend")
            try {
                npm audit --omit=dev --audit-level=high
                Assert-LastExitCode "npm audit"
            }
            finally {
                Pop-Location
            }
        }
    }

    if (-not $SkipBackend) {
        Invoke-SecurityStep "Backend dependency audit" {
            if (Get-Command pip-audit -ErrorAction SilentlyContinue) {
                pip-audit -r (Join-Path $RepoRoot "backend\requirements.txt")
                Assert-LastExitCode "pip-audit"
            }
            elseif (Test-PythonModule $BackendPython "pip_audit") {
                & $BackendPython -m pip_audit -r (Join-Path $RepoRoot "backend\requirements.txt")
                Assert-LastExitCode "python -m pip_audit"
            }
            else {
                throw "pip-audit is not installed. Install it for development with: python -m pip install pip-audit"
            }
        }
    }

    if (-not $SkipSecrets) {
        Invoke-SecurityStep "Secret scan" {
            $Gitleaks = Resolve-Gitleaks
            if (-not $Gitleaks) {
                throw "gitleaks is not installed. Install it locally, then rerun this script."
            }

            & $Gitleaks detect --source $RepoRoot --redact --no-banner --exit-code 1
            Assert-LastExitCode "gitleaks"
        }
    }
}
finally {
    Pop-Location
}
