# personal-agent

Matrix-native personal agent on claudebox. Single-operator. Polls `#personal:claudebox.me`,
spawns / resumes a stateful `claude -p` session per Matrix thread, posts responses back.

This is the canonical implementation that proves the resident-agent pattern before it
generalises to Helm.

## Layout

| Path | Purpose |
|------|---------|
| `manager.py` | Matrix poll loop, session spawn/resume, SQLite, structured logging |
| `config.yml` | Runtime config (gitignored — `config.example.yml` is the template) |
| `CLAUDE.md` | Persona + section markers (LOCKED / EDITABLE / APPEND-ONLY) |
| `start.sh` | PM2 entry — sources `~/.claude-secrets/matrix-personal.env` then execs the venv python |
| `ecosystem.config.js` | PM2 service definition |
| `requirements.txt` | Pinned dependency versions |

## Build phases

| Phase | Status | Adds |
|-------|--------|------|
| v0.1 | in progress | Single-session per thread, persona, delegation tool surface |
| v0.2 | planned | Idle-based session rollover with handoff injection |
| v0.3 | planned | Dynamic rollover triggers (idle / token-fill / age) |
| v0.4 | planned | Typing indicators, cold-start memory injection, persona refinement |
| v0.5 | planned | task-queue-mcp delegation flow + agent-bus result synthesis |
| v0.6 | planned | Gitea-backed self-modification with locked-section validation |
| v0.7 | planned | Sleep-window pipeline + 5:30 AM morning briefing |

See the build plan at `~/.claude/comms/artifacts/build-plans/personal-agent/plan.md`
for the full design.

## Operations

```bash
# View status
pm2 status personal-agent
pm2 logs personal-agent --lines 50

# Restart after config change
pm2 restart personal-agent
pm2 save
```

## Tool surface

All tool access goes through the scoped-mcp manifest at
`~/.claude/manifests/personal-agent.yml`. The manifest is the single source of
truth for what the agent can reach; argument filters block credential-shaped
strings and scope filesystem reads to the agent's own directories.
