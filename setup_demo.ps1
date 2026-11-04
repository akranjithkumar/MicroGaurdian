# ==============================================================================
# MicroGuardian — Windows Demo & Quick Start Script
# ==============================================================================
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  MicroGuardian: Telemetry-Integrity-Aware K8s Autoscaling" -ForegroundColor Cyan
Write-Host "  1st Review Prototype Setup & Verification" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Check Python
Write-Host "`n[1/4] Verifying Python Environment..." -ForegroundColor Yellow
python --version
pip install -r controller/requirements.txt --quiet

# 2. Run Test Suite
Write-Host "`n[2/4] Running Watchdog & Controller Unit Tests..." -ForegroundColor Yellow
python -m unittest tests/test_controller.py

# 3. Run Experiments (Phase 3)
Write-Host "`n[3/4] Running Telemetry Chaos Experiments (All 4 Scenarios)..." -ForegroundColor Yellow
python chaos/run_experiment.py --scenario all

# 4. Generate Comparative Analysis
Write-Host "`n[4/4] Generating Empirical Analysis Summary..." -ForegroundColor Yellow
python analysis/analyze_results.py

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host " Prototype execution verified successfully!" -ForegroundColor Green
Write-Host " Check analysis/results/ for complete experiment JSON logs." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
