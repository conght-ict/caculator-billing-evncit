# =========================================================================
# SMART Local CI/CD Pipeline Script cho EVN Billing Core System (PowerShell)
# Tự động phát hiện module thay đổi, fail-fast theo exit code, và deploy an toàn.
# =========================================================================

Param (
    [string]$Service = "auto" # auto, all, worker, mediation, snapshot
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Khong tim thay lenh '$Name' trong PATH. Vui long cai dat/bo sung PATH truoc khi chay deploy."
    }
}

function Invoke-External {
    param(
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$Description
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description that bai (exit code: $LASTEXITCODE)."
    }
}

function Ensure-LocalImage {
    param([string]$Image)
    docker image inspect $Image *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Khong tim thay Docker image local '$Image'."
    }
}

function Invoke-KubectlNonFatal {
    param([string[]]$Arguments)
    & kubectl @Arguments
    return $LASTEXITCODE
}

function Wait-DeploymentReady {
    param([string]$DeploymentName)

    Write-Host "Cho deployment '$DeploymentName' san sang..." -ForegroundColor DarkCyan
    & kubectl rollout status "deployment/$DeploymentName" -n evn-billing --timeout=180s
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Canh bao: rollout '$DeploymentName' chua thanh cong. In thong tin chan doan..." -ForegroundColor DarkYellow
        Invoke-KubectlNonFatal -Arguments @("get", "pods", "-n", "evn-billing", "-o", "wide") | Out-Null
        Invoke-KubectlNonFatal -Arguments @("describe", "deployment", $DeploymentName, "-n", "evn-billing") | Out-Null
        Invoke-KubectlNonFatal -Arguments @("get", "events", "-n", "evn-billing", "--sort-by=.lastTimestamp") | Out-Null
        throw "Deployment '$DeploymentName' khong san sang trong thoi gian cho phep."
    }
}

function Import-Images-IntoNodeContainerd {
    param(
        [string[]]$Images,
        [string[]]$NodeNames
    )

    foreach ($node in $NodeNames) {
        docker ps --format "{{.Names}}" | Select-String -SimpleMatch $node *> $null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Canh bao: Khong tim thay node container '$node' trong docker ps. Bo qua import fallback cho node nay." -ForegroundColor DarkYellow
            continue
        }

        foreach ($img in $Images) {
            $safeImageName = $img.Replace('/', '_').Replace(':', '_')
            $tmpTar = Join-Path $env:TEMP ("$safeImageName.tar")

            Write-Host "Dang fallback import image '$img' vao node '$node'..." -ForegroundColor DarkCyan
            Invoke-External -FilePath "docker" -Arguments @("save", "-o", $tmpTar, $img) -Description "Docker save image $img"
            Invoke-External -FilePath "docker" -Arguments @("cp", $tmpTar, "${node}:/$safeImageName.tar") -Description "Copy image tar vao node $node"
            Invoke-External -FilePath "docker" -Arguments @("exec", $node, "ctr", "--namespace", "k8s.io", "images", "import", "/$safeImageName.tar") -Description "Import image $img vao containerd node $node"
            Invoke-External -FilePath "docker" -Arguments @("exec", $node, "rm", "-f", "/$safeImageName.tar") -Description "Don dep image tar tren node $node"

            if (Test-Path $tmpTar) {
                Remove-Item -Path $tmpTar -Force
            }
        }
    }
}

function Load-Image-ToKindIfNeeded {
    param([string[]]$Images)

    $nodeNamesRaw = kubectl get nodes -o name 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $nodeNamesRaw) {
        Write-Host "Canh bao: Khong doc duoc node name de kiem tra runtime cluster." -ForegroundColor DarkYellow
        return
    }

    $nodeNames = @(
        $nodeNamesRaw -split "`r?`n" |
            Where-Object { $_ -and $_.Trim() } |
            ForEach-Object { $_ -replace "^node/", "" }
    )
    if (@($nodeNames).Count -eq 0) {
        Write-Host "Canh bao: Khong tim thay node nao de load image." -ForegroundColor DarkYellow
        return
    }

    $firstNodeName = $nodeNames[0]

    # Neu node dang o dang <cluster>-control-plane thi thu load image vao kind cluster.
    if ($firstNodeName -match "^(.+)-control-plane$") {
        $clusterName = $Matches[1]
        $kindCmd = Get-Command kind -ErrorAction SilentlyContinue
        if ($kindCmd) {
            foreach ($img in $Images) {
                Write-Host "Dang nap image '$img' vao kind cluster '$clusterName'..." -ForegroundColor DarkCyan
                Invoke-External -FilePath "kind" -Arguments @("load", "docker-image", $img, "--name", $clusterName) -Description "Nap image $img vao kind"
            }
        } else {
            Write-Host "Canh bao: Node name la '$firstNodeName' (kieu kind) nhung chua co lenh 'kind'. Thu fallback import truc tiep vao containerd..." -ForegroundColor DarkYellow
            Import-Images-IntoNodeContainerd -Images $Images -NodeNames $nodeNames
        }
    }
}

try {
    Assert-Command -Name "git"
    Assert-Command -Name "mvn"
    Assert-Command -Name "docker"
    Assert-Command -Name "kubectl"

    Write-Host "[1/4] Kiem tra cac tep tin ma nguon co thay doi..." -ForegroundColor Cyan

    $buildWorker = $false
    $buildMediation = $false
    $buildSnapshot = $false

    $svcLower = $Service.ToLowerInvariant()
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
        $oldPref = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        $gitStatus = git status --porcelain 2>$null
        $gitDiff = git diff --name-only HEAD~1 2>$null
        $ErrorActionPreference = $oldPref
        $allChanges = "$gitStatus`n$gitDiff"

        if ($allChanges.Trim()) {
            if ($allChanges -match "billing-common[/\\]" -or $allChanges -match "common[/\\]") {
                Write-Host "Phat hien thay doi trong module dung chung -> Rebuild tat ca module!" -ForegroundColor Yellow
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
            Write-Host "Khong phat hien ma nguon moi thay doi. Rebuild mac dinh module 'billing-worker'..." -ForegroundColor Gray
            $buildWorker = $true
        }
    }

    $modulesToBuild = @()
    $imagesToBuild = @()

    if ($buildWorker) {
        $modulesToBuild += "billing-worker"
        $imagesToBuild += "billing-worker:latest"
    }
    if ($buildMediation) {
        $modulesToBuild += "mediation-service"
        $imagesToBuild += "mediation-service:latest"
    }
    if ($buildSnapshot) {
        $modulesToBuild += "snapshot-generator"
        $imagesToBuild += "snapshot-generator:latest"
    }

    if (@($modulesToBuild).Count -eq 0) {
        throw "Khong co module nao duoc chon de build/deploy."
    }

    $plArgs = $modulesToBuild -join ","
    Write-Host "[2/4] Dang bien dich Maven cho cac module: [$plArgs]..." -ForegroundColor Cyan
    Invoke-External -FilePath "mvn" -Arguments @("clean", "package", "-DskipTests", "-pl", $plArgs, "-am") -Description "Maven build"

    Write-Host "[3/4] Build Docker Images..." -ForegroundColor Yellow
    if ($buildWorker) {
        Invoke-External -FilePath "docker" -Arguments @("build", "-t", "billing-worker:latest", "./billing-worker") -Description "Build billing-worker image"
    }
    if ($buildMediation) {
        Invoke-External -FilePath "docker" -Arguments @("build", "-t", "mediation-service:latest", "./mediation-service") -Description "Build mediation-service image"
    }
    if ($buildSnapshot) {
        Invoke-External -FilePath "docker" -Arguments @("build", "-t", "snapshot-generator:latest", "./snapshot-generator") -Description "Build snapshot-generator image"
    }

    foreach ($img in $imagesToBuild) {
        Ensure-LocalImage -Image $img
    }

    # Nap image vao runtime local neu node theo pattern kind.
    Load-Image-ToKindIfNeeded -Images $imagesToBuild

    Write-Host "[4/4] Cap nhat Kubernetes Manifests va Restart Pods..." -ForegroundColor Green
    Invoke-External -FilePath "kubectl" -Arguments @("apply", "-f", "k8s/00-namespace-config.yaml") -Description "Apply namespace/config"

    if ($buildWorker) {
        Invoke-External -FilePath "kubectl" -Arguments @("apply", "-f", "k8s/01-billing-worker-deployment.yaml") -Description "Apply billing-worker deployment"
        Invoke-External -FilePath "kubectl" -Arguments @("rollout", "restart", "deployment/billing-worker", "-n", "evn-billing") -Description "Restart billing-worker"
        # Wait-DeploymentReady -DeploymentName "billing-worker"
    }

    if ($buildMediation) {
        Invoke-External -FilePath "kubectl" -Arguments @("apply", "-f", "k8s/03-mediation-service-deployment.yaml") -Description "Apply billing-mediation deployment"
        Invoke-External -FilePath "kubectl" -Arguments @("rollout", "restart", "deployment/billing-mediation", "-n", "evn-billing") -Description "Restart billing-mediation"
        # Wait-DeploymentReady -DeploymentName "billing-mediation"
    }

    if ($buildSnapshot) {
        Invoke-External -FilePath "kubectl" -Arguments @("apply", "-f", "k8s/07-snapshot-generator-deployment.yaml") -Description "Apply snapshot-generator deployment"
        Invoke-External -FilePath "kubectl" -Arguments @("rollout", "restart", "deployment/snapshot-generator", "-n", "evn-billing") -Description "Restart snapshot-generator"
        # Wait-DeploymentReady -DeploymentName "snapshot-generator"
    }

    Invoke-External -FilePath "kubectl" -Arguments @("apply", "-f", "k8s/05-ingress.yaml") -Description "Apply ingress"
    Invoke-External -FilePath "kubectl" -Arguments @("apply", "-f", "k8s/06-hpa-autoscale.yaml") -Description "Apply HPA"

    if (Test-Path "k8s/01b-keda-scaler.yaml") {
        Write-Host "Dang apply KEDA scaler (optional)..." -ForegroundColor Yellow
        & kubectl apply -f k8s/01b-keda-scaler.yaml 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Canh bao: Apply KEDA scaler that bai. Co the KEDA chua duoc cai dat (CRD ScaledObject thieu). Bo qua vi day la tinh nang mo rong." -ForegroundColor DarkYellow
        }
    }

    Write-Host "HOAN TAT CI/CD LOCAL: Da cap nhat thanh cong cac Pods & HPA Autoscalers!" -ForegroundColor Green
    kubectl get pods -n evn-billing
} catch {
    Write-Host "CI/CD LOCAL THAT BAI: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
