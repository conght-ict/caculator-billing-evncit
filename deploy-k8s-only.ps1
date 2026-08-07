Param (
    [string]$Service = "all", # all, worker, mediation, snapshot
    [switch]$NoRestart,
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Khong tim thay lenh '$Name' trong PATH."
    }
}

function Invoke-Kubectl {
    param(
        [string[]]$Arguments,
        [string]$Description
    )

    & kubectl @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description that bai (exit code: $LASTEXITCODE)."
    }
}

function Wait-DeploymentReady {
    param([string]$DeploymentName)

    & kubectl rollout status "deployment/$DeploymentName" -n evn-billing --timeout ("{0}s" -f $TimeoutSeconds)
    if ($LASTEXITCODE -ne 0) {
        throw "Deployment '$DeploymentName' chua san sang trong $TimeoutSeconds giay."
    }
}

function Apply-ServiceManifest {
    param(
        [string]$Manifest,
        [string]$DeploymentName,
        [switch]$DoRestart
    )

    Invoke-Kubectl -Arguments @("apply", "-f", $Manifest) -Description "Apply $Manifest"
    if ($DoRestart) {
        Invoke-Kubectl -Arguments @("rollout", "restart", "deployment/$DeploymentName", "-n", "evn-billing") -Description "Restart $DeploymentName"
    }
    Wait-DeploymentReady -DeploymentName $DeploymentName
}

try {
    Assert-Command -Name "kubectl"

    $svcLower = $Service.ToLowerInvariant()
    $deployWorker = $false
    $deployMediation = $false
    $deploySnapshot = $false

    switch ($svcLower) {
        "all" {
            $deployWorker = $true
            $deployMediation = $true
            $deploySnapshot = $true
        }
        "worker" { $deployWorker = $true }
        "billing-worker" { $deployWorker = $true }
        "mediation" { $deployMediation = $true }
        "mediation-service" { $deployMediation = $true }
        "snapshot" { $deploySnapshot = $true }
        "snapshot-generator" { $deploySnapshot = $true }
        default { throw "Gia tri -Service khong hop le: $Service" }
    }

    Write-Host "[1/3] Apply namespace/config..." -ForegroundColor Cyan
    Invoke-Kubectl -Arguments @("apply", "-f", "k8s/00-namespace-config.yaml") -Description "Apply namespace/config"

    Write-Host "[2/3] Apply deployment manifests..." -ForegroundColor Cyan
    $doRestart = -not $NoRestart

    if ($deployWorker) {
        Apply-ServiceManifest -Manifest "k8s/01-billing-worker-deployment.yaml" -DeploymentName "billing-worker" -DoRestart:$doRestart
    }
    if ($deployMediation) {
        Apply-ServiceManifest -Manifest "k8s/03-mediation-service-deployment.yaml" -DeploymentName "billing-mediation" -DoRestart:$doRestart
    }
    if ($deploySnapshot) {
        Apply-ServiceManifest -Manifest "k8s/07-snapshot-generator-deployment.yaml" -DeploymentName "snapshot-generator" -DoRestart:$doRestart
    }

    Write-Host "[3/3] Apply ingress/autoscaling..." -ForegroundColor Cyan
    Invoke-Kubectl -Arguments @("apply", "-f", "k8s/05-ingress.yaml") -Description "Apply ingress"
    Invoke-Kubectl -Arguments @("apply", "-f", "k8s/06-hpa-autoscale.yaml") -Description "Apply HPA"

    if (Test-Path "k8s/01b-keda-scaler.yaml") {
        & kubectl apply -f k8s/01b-keda-scaler.yaml 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Canh bao: Khong apply duoc KEDA scaler (co the chua cai CRD)." -ForegroundColor DarkYellow
        }
    }

    Write-Host "HOAN TAT K8S-ONLY DEPLOY." -ForegroundColor Green
    & kubectl get pods -n evn-billing
} catch {
    Write-Host "K8S-ONLY DEPLOY THAT BAI: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
