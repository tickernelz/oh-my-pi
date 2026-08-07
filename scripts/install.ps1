# Downstream OMP LCM Installer for Windows source builds
# Usage: & ([scriptblock]::Create((irm https://raw.githubusercontent.com/tickernelz/oh-my-pi/main/scripts/install.ps1))) -Source
#
# Native downstream release binaries currently support Linux x64/WSL only.
# Run install.sh inside WSL for the default binary install. Windows users may
# pass -Source (and optionally -Ref) to clone tickernelz/oh-my-pi explicitly.

param(
    [switch]$Source,
    [switch]$Binary,
    [string]$Ref
)

$ErrorActionPreference = "Stop"

$Repo = "tickernelz/oh-my-pi"
$InstallDir = if ($env:PI_INSTALL_DIR) { $env:PI_INSTALL_DIR } else { "$env:LOCALAPPDATA\omp" }
$MinimumBunVersion = "1.3.14"

function Test-BunInstalled {
    try {
        $null = Get-Command bun -ErrorAction Stop
        return $true
    } catch {
        return $false
    }
}

function Get-BunVersion {
    try {
        $versionText = (bun --version 2>$null)
        if (-not $versionText) {
            return $null
        }

        $clean = $versionText.Trim().Split("-")[0]
        return [version]$clean
    } catch {
        return $null
    }
}

function Test-BunVersion {
    param([string]$MinimumVersion)

    $currentVersion = Get-BunVersion
    if (-not $currentVersion) {
        return $false
    }

    return $currentVersion -ge [version]$MinimumVersion
}

function Assert-BunVersion {
    param([string]$MinimumVersion)

    if (-not (Test-BunVersion $MinimumVersion)) {
        $current = Get-BunVersion
        $currentText = if ($current) { $current.ToString() } else { "unknown" }
        throw "Bun $MinimumVersion or newer is required. Current version: $currentText. Upgrade Bun at https://bun.sh/docs/installation"
    }
}

function Test-GitInstalled {
    try {
        $null = Get-Command git -ErrorAction Stop
        return $true
    } catch {
        return $false
    }
}

function Test-GitLfsInstalled {
    try {
        $null = Get-Command git-lfs -ErrorAction Stop
        return $true
    } catch {
        return $false
    }
}

function Find-BashShell {
    # Check Git Bash first (most common on Windows)
    $gitBash = "C:\Program Files\Git\bin\bash.exe"
    if (Test-Path $gitBash) {
        return $gitBash
    }

    # Check bash.exe on PATH (Cygwin, MSYS2, WSL)
    try {
        $bashCmd = Get-Command bash.exe -ErrorAction Stop
        return $bashCmd.Source
    } catch {
        return $null
    }
}

function Configure-BashShell {
    try {
        $settingsDir = Join-Path $env:USERPROFILE ".omp\agent"
        $settingsFile = Join-Path $settingsDir "settings.json"

        # Check if settings.json already has a shellPath configured
        if (Test-Path $settingsFile) {
            try {
                $existingSettings = Get-Content $settingsFile -Raw | ConvertFrom-Json
                if ($existingSettings.shellPath) {
                    Write-Host "Bash shell already configured: $($existingSettings.shellPath)" -ForegroundColor Cyan
                    return
                }
            } catch {
                # Invalid JSON, we'll overwrite it
            }
        }

        $bashPath = Find-BashShell

        if ($bashPath) {
            Write-Host "Found bash shell: $bashPath" -ForegroundColor Cyan

            # Create settings directory if needed
            if (-not (Test-Path $settingsDir)) {
                New-Item -ItemType Directory -Force -Path $settingsDir | Out-Null
            }

            # Read existing settings or create new. ConvertFrom-Json -AsHashtable
            # requires PowerShell 6+; build the hashtable manually so Windows
            # PowerShell 5.1 merges instead of clobbering existing settings.
            $settings = @{}
            if (Test-Path $settingsFile) {
                try {
                    $parsed = Get-Content $settingsFile -Raw | ConvertFrom-Json
                    foreach ($prop in $parsed.PSObject.Properties) {
                        $settings[$prop.Name] = $prop.Value
                    }
                } catch {
                    $settings = @{}
                }
            }

            # Set shellPath
            $settings["shellPath"] = $bashPath

            # Write settings
            $settings | ConvertTo-Json -Depth 10 | Set-Content $settingsFile -Encoding UTF8
            Write-Host "[OK] Configured shell path in $settingsFile" -ForegroundColor Green
        } else {
            Write-Host ""
            Write-Host "No bash shell found - OMP will use its built-in shell." -ForegroundColor Cyan
            Write-Host "  For shell snapshots and interactive terminals, install Git for Windows:" -ForegroundColor Cyan
            Write-Host "    https://git-scm.com/download/win" -ForegroundColor Cyan
            Write-Host "  Or set a custom path in:" -ForegroundColor Cyan
            Write-Host "    $settingsFile" -ForegroundColor Cyan
            Write-Host '    { "shellPath": "C:\\path\\to\\bash.exe" }' -ForegroundColor Cyan
        }
    } catch {
        Write-Host "[WARN] Could not configure bash shell: $_" -ForegroundColor Yellow
    }
}

function Install-Bun {
    Write-Host "Installing bun..."
    irm bun.sh/install.ps1 | iex
    # Refresh PATH
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "User") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "Machine")
    Assert-BunVersion $MinimumBunVersion
}

function Install-FromSource {
    Write-Host "Installing cloned downstream source..."
    if (-not (Test-GitInstalled)) {
        throw "git is required for -Source"
    }

    $tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("omp-install-" + [System.Guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Force -Path $tmpRoot | Out-Null
    try {
        $repoUrl = "https://github.com/$Repo.git"
        if ($Ref) {
            & git clone --depth 1 --branch $Ref $repoUrl $tmpRoot | Out-Null
            if ($LASTEXITCODE -ne 0) {
                Remove-Item -Recurse -Force $tmpRoot -ErrorAction SilentlyContinue
                New-Item -ItemType Directory -Force -Path $tmpRoot | Out-Null
                & git clone $repoUrl $tmpRoot | Out-Null
                if ($LASTEXITCODE -ne 0) { throw "Failed to clone $repoUrl" }
                Push-Location $tmpRoot
                try {
                    & git checkout $Ref | Out-Null
                    if ($LASTEXITCODE -ne 0) { throw "Failed to check out downstream ref $Ref" }
                } finally {
                    Pop-Location
                }
            }
        } else {
            & git clone --depth 1 $repoUrl $tmpRoot | Out-Null
            if ($LASTEXITCODE -ne 0) { throw "Failed to clone $repoUrl" }
        }

        if (Test-GitLfsInstalled) {
            Push-Location $tmpRoot
            try { & git lfs pull | Out-Null } finally { Pop-Location }
        }

        $packagePath = Join-Path $tmpRoot "packages\coding-agent"
        if (-not (Test-Path $packagePath)) {
            throw "Expected downstream coding-agent package at $packagePath"
        }

        Push-Location $tmpRoot
        try {
            & bun install --frozen-lockfile
            if ($LASTEXITCODE -ne 0) { throw "Failed to install downstream workspace dependencies" }
        } finally {
            Pop-Location
        }
        Push-Location $packagePath
        try {
            & bun run build
            if ($LASTEXITCODE -ne 0) { throw "Failed to build downstream source" }
        } finally {
            Pop-Location
        }

        $builtPath = @(
            (Join-Path $packagePath "dist\omp.exe"),
            (Join-Path $packagePath "dist\omp")
        ) | Where-Object { Test-Path $_ } | Select-Object -First 1
        if (-not $builtPath) { throw "Downstream build produced no omp executable" }
        $reported = (& $builtPath --version 2>$null | Select-Object -Last 1)
        if (-not $reported -or $reported -notmatch '^(?:omp/)?((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)-lcm\.(?:0|[1-9]\d*))$') {
            throw "Built source did not report a downstream LCM version: $reported"
        }
        $expectedVersion = $Matches[1]

        New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
        $outPath = Join-Path $InstallDir "omp.exe"
        $tempPath = "$outPath.new.$([System.Guid]::NewGuid().ToString('N'))"
        $backupPath = $null
        Copy-Item $builtPath $tempPath
        try {
            if (Test-Path $outPath) {
                $backupPath = "$outPath.$([System.Guid]::NewGuid().ToString('N')).bak"
                Move-Item $outPath $backupPath
            }
            Move-Item $tempPath $outPath
            $installedVersion = (& $outPath --version 2>$null | Select-Object -Last 1)
            if (
                -not $installedVersion -or
                $installedVersion -notmatch '^(?:omp/)?((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)-lcm\.(?:0|[1-9]\d*))$' -or
                $Matches[1] -ne $expectedVersion
            ) {
                throw "Installed binary did not report expected version $expectedVersion"
            }
            if ($backupPath) { Remove-Item -Force $backupPath }
        } catch {
            if ($backupPath -and (Test-Path $backupPath)) {
                Remove-Item -Force $outPath -ErrorAction SilentlyContinue
                Move-Item $backupPath $outPath
            } elseif (-not $backupPath) {
                Remove-Item -Force $outPath -ErrorAction SilentlyContinue
            }
            throw "Source install failed verification; the previous executable was restored. $_"
        } finally {
            Remove-Item -Force $tempPath -ErrorAction SilentlyContinue
        }
    } finally {
        Remove-Item -Recurse -Force $tmpRoot -ErrorAction SilentlyContinue
    }

    Write-Host ""
    Write-Host "[OK] Installed downstream omp from $Repo" -ForegroundColor Green
    Configure-BashShell
    Write-Host "Run 'omp' to get started!"
}

function Install-Binary {
    throw "Downstream LCM release binaries currently support Linux x64 (including WSL) only. Run the downstream install.sh inside WSL, or re-run this installer with -Source to clone tickernelz/oh-my-pi."
}

# Main logic
if ($Source -and $Binary) {
    throw "Choose exactly one of -Source or -Binary"
}

if ($Source) {
    if (-not (Test-BunInstalled)) {
        Install-Bun
    }
    Assert-BunVersion $MinimumBunVersion
    Install-FromSource
} else {
    Install-Binary
}
