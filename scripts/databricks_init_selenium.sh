#!/usr/bin/env bash
set -euo pipefail

apt-get update
apt-get install -y chromium-browser chromium-chromedriver

mkdir -p /databricks/driver
ln -sf /usr/bin/chromedriver /databricks/driver/chromedriver
