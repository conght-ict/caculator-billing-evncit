# =========================================================================
# SMART Local CI/CD Pipeline Script cho EVN Billing Core System (PowerShell)
# Tự động phát hiện các module có thay đổi code để build va deploy tối ưu!
# =========================================================================

Param (
    [string]$Service = "auto" # auto, all, worker, mediation, snapshot
)

Write-Host "[1/4] Kiem tra cac tep tin ma nguon co thay doi..." -ForegroundColor Cyan

$buildWorker = $false
$buildMediation = $false
$buildSnapshot = $false

$svcLower = $Service.ToLower()
if ($svcLower -eq "all") {
    $buildWorker = $true
    $buildMediation = $true
    $buildSnapshot = $true
} elseif ($svcLower -eq "worker" -or $svcLower -eq "billing-worker") {
    $buildWorker = $true
} elseif ($svcLower -eq "mediation" -or $svcLower -eq "mediation-service" -or $svcLower -eq "orchestrator" -or $svcLower -eq "batch-orchestrator") {
    $buildMediation = $true
} elseif ($svcLower -eq "snapshot" -or $svcLower -eq "snapshot-generator") {
    $buildSnapshot = $true
} else {
    $gitStatus = git status --porcelain 2>$null
    $gitDiff = git diff --name-only HEAD~1 2>$null
    $allChanges = "$gitStatus`n$gitDiff"

    if ($allChanges.Trim()) {
        if ($allChanges -match "common[/\\]") {
            Write-Host "Phat hien thay doi trong thu muc chung 'common' -> Rebuild tat ca module!" -ForegroundColor Yellow
            $buildWorker = $true
            $buildMediation = $true
            $buildSnapshot = $true
        } else {
            if ($allChanges -match "billing-worker[/\\]") {
                Write-Host "Phat hien thay doi trong module 'billing-worker'" -ForegroundColor Green
                $buildWorker = $true
            }
            if ($allChanges -match "mediation-service[/\\]" -or $allChanges -match "mediation[/\\]" -or $allChanges -match "batch-orchestrator[/\\]") {
                Write-Host "Phat hien thay doi trong module 'mediation-service'" -ForegroundColor Green
                $buildMediation = $true
            }
            if ($allChanges -match "snapshot-generator[/\\]" -or $allChanges -match "snapshot[/\\]") {
                Write-Host "Phat hien thay doi trong module 'snapshot-generator'" -ForegroundColor Green
                $buildSnapshot = $true
            }
        }
    }
    
    if (-not $buildWorker -and -not $buildMediation -and -not $buildSnapshot) {
        Write-Host "Khong phat hien ma nguon moi thay doi. Rebuild module mac dinh 'billing-worker'..." -ForegroundColor Gray
        $buildWorker = $true
    }
}

# 2. Bien dich Maven
$modulesToBuild = @()
if ($buildWorker) { $modulesToBuild += "billing-worker" }
if ($buildMediation) { $modulesToBuild += "mediation-service" }
if ($buildSnapshot) { $modulesToBuild += "snapshot-generator" }

$plArgs = $modulesToBuild -join ","
Write-Host "[2/4] Dang bien dich Maven cho cac module: [$plArgs]..." -ForegroundColor Cyan
mvn clean package -DskipTests -pl $plArgs -am

# 3. Build Docker Image
if ($buildWorker) {
    Write-Host "[3/4] Build Docker Image: billing-worker:latest..." -ForegroundColor Yellow
    docker build -t billing-worker:latest ./billing-worker
}

if ($buildMediation) {
    Write-Host "[3/4] Build Docker Image: mediation-service:latest..." -ForegroundColor Yellow
    docker build -t mediation-service:latest ./mediation-service
}

if ($buildSnapshot) {
    Write-Host "[3/4] Build Docker Image: snapshot-generator:latest..." -ForegroundColor Yellow
    docker build -t snapshot-generator:latest ./snapshot-generator
}

# 4. Deploy va Restart Pods
Write-Host "[4/4] Cap nhat Kubernetes Manifests va Restart Pods..." -ForegroundColor Green
kubectl apply -f k8s/00-namespace-config.yaml

if ($buildWorker) {
    kubectl apply -f k8s/01-billing-worker-deployment.yaml
    kubectl rollout restart deployment/billing-worker -n evn-billing
}

if ($buildMediation) {
    kubectl apply -f k8s/03-mediation-service-deployment.yaml
    kubectl rollout restart deployment/billing-mediation -n evn-billing
}

if ($buildSnapshot -or (Test-Path "k8s/07-snapshot-generator-deployment.yaml")) {
    kubectl apply -f k8s/07-snapshot-generator-deployment.yaml
    kubectl rollout restart deployment/snapshot-generator -n evn-billing
}

# Apply Ingress, HPA Autoscale & KEDA Scaler manifests
kubectl apply -f k8s/05-ingress.yaml
kubectl apply -f k8s/06-hpa-autoscale.yaml
if (Test-Path "k8s/01b-keda-scaler.yaml") {
    kubectl apply -f k8s/01b-keda-scaler.yaml -ErrorAction SilentlyContinue
}

Write-Host "HOAN TAT CI/CD LOCAL: Da cap nhat thanh cong cac Pods & HPA Autoscalers!" -ForegroundColor Green
kubectl get pods -n evn-billing
