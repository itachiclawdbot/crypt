#!/usr/bin/env bash
set -euo pipefail
cd /home/itachi/.openclaw/workspace
exec python3 -m project_crypt.phase2_ops_dashboard
