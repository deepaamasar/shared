# Build the ctx_worker_shared wheel for distribution into worker deploy bundles.
#
# Usage:  .\build-shared.ps1
# Output: dist\ctx_worker_shared-<version>-py3-none-any.whl
#
# The wheel is the ONLY artifact that ships to VMs. It is copied into each
# worker's deploy bundle; that worker's requirements file installs it by local
# path (no git, no index). Bump [project].version in pyproject.toml on any
# change, rebuild, and redistribute the wheel to every bundle.

$ErrorActionPreference = "Stop"

Write-Host "Cleaning previous build artifacts..."
Remove-Item -Recurse -Force build, dist, src\*.egg-info -ErrorAction SilentlyContinue

Write-Host "Ensuring 'build' is available..."
python -m pip install --upgrade build | Out-Host

Write-Host "Building wheel..."
python -m build --wheel | Out-Host

Write-Host ""
Write-Host "Built:" -ForegroundColor Green
Get-ChildItem dist\*.whl | ForEach-Object { Write-Host "  $($_.FullName)" }
Write-Host ""
Write-Host "Next: copy the .whl into each worker deploy bundle (see worker DEPLOY.md)."
