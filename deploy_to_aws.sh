#!/bin/bash
set -e

HOST_IP="${1:-13.60.250.242}"
USER="${2:-ubuntu}"
PEM_PATH="${3:-/d/Downloads/Real-Time Streaming Dashboard.pem}"
REMOTE_DIR="~/Real-time-streaming-dashboard"

echo "============================================================"
echo " AWS EC2 DEPLOYMENT PIPELINE FOR STREAMING DASHBOARD"
echo "============================================================"
echo "Target Host : ${USER}@${HOST_IP}"
echo "PEM Key     : ${PEM_PATH}"
echo "Remote Path : ${REMOTE_DIR}"
echo ""

if [ ! -f "${PEM_PATH}" ]; then
    echo "[-] PEM key not found at '${PEM_PATH}'."
    exit 1
fi

chmod 400 "${PEM_PATH}" 2>/dev/null || true

echo "=== 1. Testing SSH Connectivity to ${HOST_IP} ==="
if ! ssh -i "${PEM_PATH}" -o ConnectTimeout=6 -o StrictHostKeyChecking=no "${USER}@${HOST_IP}" "echo 'SSH_CONNECTED'" >/dev/null 2>&1; then
    echo ""
    echo "[!] Unable to connect to ${HOST_IP} via SSH on port 22."
    echo "Possible causes:"
    echo " 1. The EC2 instance is currently STOPPED in the AWS Management Console."
    echo " 2. The EC2 instance was restarted and assigned a NEW Public IPv4 address."
    echo " 3. The AWS Security Group does not allow inbound Port 22 (SSH) from your current IP."
    echo " 4. The SSH username might be 'ec2-user' instead of 'ubuntu'."
    echo ""
    echo "Usage with custom IP: ./deploy_to_aws.sh <YOUR_EC2_PUBLIC_IP>"
    exit 1
fi

echo "[+] SSH connection confirmed!"

echo "=== 2. Preparing Remote Directory ==="
ssh -i "${PEM_PATH}" -o StrictHostKeyChecking=no "${USER}@${HOST_IP}" "mkdir -p ${REMOTE_DIR}/ml ${REMOTE_DIR}/database ${REMOTE_DIR}/spark ${REMOTE_DIR}/producer ${REMOTE_DIR}/config ${REMOTE_DIR}/docker ${REMOTE_DIR}/tests"

echo "=== 3. Uploading Codebase to EC2 via SCP ==="
ITEMS=("config" "dashboard" "database" "docker" "ml" "producer" "spark" "tests" "deploy_remote.sh" "requirements.txt" "README.md" "IMPLEMENTATION_REPORT.md")

for item in "${ITEMS[@]}"; do
    if [ -e "$item" ]; then
        echo "Uploading $item -> ${REMOTE_DIR}/$item ..."
        scp -i "${PEM_PATH}" -r -o StrictHostKeyChecking=no "$item" "${USER}@${HOST_IP}:${REMOTE_DIR}/"
    fi
done

echo "[+] Upload completed!"

echo "=== 4. Executing Remote Deployment Script on EC2 ==="
ssh -i "${PEM_PATH}" -o StrictHostKeyChecking=no "${USER}@${HOST_IP}" "cd ${REMOTE_DIR} && chmod +x deploy_remote.sh && ./deploy_remote.sh"

echo ""
echo "============================================================"
echo " DEPLOYMENT TO AWS COMPLETED SUCCESSFULLY!"
echo "Live Grafana URL: http://${HOST_IP}:3000"
echo "Kafka UI URL    : http://${HOST_IP}:8080"
echo "Spark UI URL    : http://${HOST_IP}:8081"
echo "============================================================"
