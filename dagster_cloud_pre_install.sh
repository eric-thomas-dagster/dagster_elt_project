#!/bin/bash
# Dagster Cloud pre-install hook
# This runs BEFORE Python dependencies are installed
# Use this to install system-level dependencies

set -e

echo "Installing system dependencies (git)..."

apt-get update
apt-get install -y \
    git \
    build-essential

apt-get clean
rm -rf /var/lib/apt/lists/*

echo "System dependencies installed successfully!"

# Verify git is installed
git --version
