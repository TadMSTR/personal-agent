#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="/home/ted/.claude-secrets/matrix-personal.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: credentials file not found: $ENV_FILE" >&2
    exit 1
fi

set -o allexport
# shellcheck disable=SC1090
source "$ENV_FILE"
set +o allexport

exec /home/ted/repos/personal/personal-agent/.venv/bin/python \
    /home/ted/repos/personal/personal-agent/manager.py
