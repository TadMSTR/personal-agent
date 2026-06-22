#!/usr/bin/env bash
# Launch the personal-agent manager. Loads bot credentials from an env file
# (chmod 600), selects the deployment config, and execs the daemon in its venv.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_FILE="${PERSONAL_AGENT_ENV:-$HOME/.claude-secrets/personal-agent.env}"

if [[ ! -f "$SECRETS_FILE" ]]; then
  echo "Missing credentials file: $SECRETS_FILE" >&2
  exit 1
fi
# shellcheck disable=SC1090
set -a; source "$SECRETS_FILE"; set +a

export PERSONAL_AGENT_CONFIG="${PERSONAL_AGENT_CONFIG:-$REPO_DIR/config.harlock.yml}"

exec "$REPO_DIR/venv/bin/python" "$REPO_DIR/manager.py"
