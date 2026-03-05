#!/bin/bash

# 1. Load the variables from the .env file
if [ -f .env ]; then
    export $(echo $(cat .env | sed 's/#.*//g' | xargs) | envsubst)
    # OR simply use: source .env 
    # (Note: 'source' only works if the .env file follows shell syntax)
else
    echo ".env file not found!"
    exit 1
fi

# Configuration
LOCAL_DIR="/home/chethan/Desktop/IISC/courses/sem2/SSML/asssignment/scalability_study/"
REMOTE_USER="chethan1"
REMOTE_HOST="10.24.1.10"
REMOTE_DIR="/scratch/chethan1/SSDS/scalability_study/"
REMOTE_PASS="Rock@222"  # Set your password here

echo "------------------------------------------------"
echo "Sending files to IISc Cluster..."
echo "------------------------------------------------"

# Run rsync using sshpass to provide the password
sshpass -p "$REMOTE_PASS" rsync -avz --progress -e ssh "$LOCAL_DIR" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}"

if [ $? -eq 0 ]; then
    echo "------------------------------------------------"
    echo "Successfully synced to cluster."
else
    echo "Error: Transfer failed. Check your VPN/SSH connection."
fi