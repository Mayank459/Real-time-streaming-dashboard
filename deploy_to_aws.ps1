param (
    [string]$HostIp = "13.60.250.242",
    [string]$User = "ubuntu",
    [string]$PemKeyPath = "D:\Downloads\Real-Time Streaming Dashboard.pem",
    [string]$RemoteDir = "~/Real-time-streaming-dashboard"
)

$ErrorActionPreference = "Stop"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " AWS EC2 DEPLOYMENT PIPELINE FOR STREAMING DASHBOARD" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Target Host : $User@$HostIp"
Write-Host "PEM Key     : $PemKeyPath"
Write-Host "Remote Path : $RemoteDir"
Write-Host ""

# Verify PEM Key exists
if (-not (Test-Path $PemKeyPath)) {
    Write-Error "PEM key not found at '$PemKeyPath'. Please check the file path."
    exit 1
}

# Test SSH connectivity
Write-Host "=== 1. Testing SSH Connectivity to $HostIp ===" -ForegroundColor Yellow
$sshConnected = $false
try {
    $sshOutput = & ssh -i $PemKeyPath -o ConnectTimeout=6 -o StrictHostKeyChecking=no "$User@$HostIp" "echo 'SSH_CONNECTED'" 2>$null
    if ($sshOutput -match "SSH_CONNECTED") {
        $sshConnected = $true
    }
} catch {
    $sshConnected = $false
}

if (-not $sshConnected) {
    Write-Host ""
    Write-Host "[!] Unable to connect to $HostIp via SSH on port 22." -ForegroundColor Red
    Write-Host "Possible causes:" -ForegroundColor Red
    Write-Host " 1. The EC2 instance is currently STOPPED in the AWS Management Console." -ForegroundColor Yellow
    Write-Host " 2. The EC2 instance was restarted and assigned a NEW Public IPv4 address." -ForegroundColor Yellow
    Write-Host " 3. The AWS Security Group does not allow inbound Port 22 (SSH) from your current IP." -ForegroundColor Yellow
    Write-Host " 4. The SSH username might be 'ec2-user' instead of 'ubuntu'." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "If you have the updated EC2 Public IP, run:" -ForegroundColor Cyan
    Write-Host "  .\deploy_to_aws.ps1 -HostIp <YOUR_EC2_PUBLIC_IP>" -ForegroundColor White
    exit 1
}

Write-Host "[+] SSH connection confirmed!" -ForegroundColor Green

# Ensure remote directory exists
Write-Host "=== 2. Preparing Remote Directory ===" -ForegroundColor Yellow
ssh -i $PemKeyPath -o StrictHostKeyChecking=no "$User@$HostIp" "mkdir -p $RemoteDir/ml $RemoteDir/database $RemoteDir/spark $RemoteDir/producer $RemoteDir/config $RemoteDir/docker $RemoteDir/tests"

# Upload updated directories and files
Write-Host "=== 3. Uploading Codebase to EC2 via SCP ===" -ForegroundColor Yellow
$itemsToUpload = @(
    "config",
    "dashboard",
    "database",
    "docker",
    "ml",
    "producer",
    "spark",
    "tests",
    "deploy_remote.sh",
    "requirements.txt",
    "README.md",
    "IMPLEMENTATION_REPORT.md"
)

foreach ($item in $itemsToUpload) {
    if (Test-Path $item) {
        Write-Host "Uploading $item -> $RemoteDir/$item ..." -ForegroundColor Gray
        scp -i $PemKeyPath -r -o StrictHostKeyChecking=no $item "$User@${HostIp}:$RemoteDir/"
    }
}

Write-Host "✅ Upload completed!" -ForegroundColor Green

# Make remote scripts executable and trigger deployment
Write-Host "=== 4. Executing Remote Deployment Script on EC2 ===" -ForegroundColor Yellow
ssh -i $PemKeyPath -o StrictHostKeyChecking=no "$User@$HostIp" "cd $RemoteDir && chmod +x deploy_remote.sh && ./deploy_remote.sh"

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "🎉 DEPLOYMENT TO AWS COMPLETED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "Live Grafana URL: http://${HostIp}:3000" -ForegroundColor Green
Write-Host "Kafka UI URL    : http://${HostIp}:8080" -ForegroundColor Green
Write-Host "Spark UI URL    : http://${HostIp}:8081" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
