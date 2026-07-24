---
title: "Fly.io Postgres password drift breaks GitHub Actions canonical check"
date: "2026-07-11"
category: integration-issues
module: ci-cd
problem_type: integration_issue
component: tooling
symptoms:
  - "Canonical drift check workflow fails daily with password authentication failed for user postgres"
  - "verify_canonical.py cannot connect to cinderhaven-db through flyctl proxy"
  - "Workflow passed through July 10, started failing July 11 with no code changes"
root_cause: config_error
resolution_type: config_change
severity: high
tags:
  - flyio
  - postgres
  - github-actions
  - secrets-management
  - ci-cd
  - credential-drift
  - canonical-drift
---

# Fly.io Postgres password drift breaks GitHub Actions canonical check

## Problem

The "Canonical drift check" GitHub Actions workflow in cinderhaven-data-platform started failing on July 11, 2026 after 4+ days of passing runs. The workflow connects to a Fly.io Postgres database via flyctl proxy and runs `verify_canonical.py` to verify canonical business metrics haven't drifted from the live database. The `POSTGRES_PASSWORD` GitHub Actions secret no longer matched the actual database password.

## Symptoms

- `gh run list --workflow=canonical-drift.yml` showed 3 consecutive failures (July 11, 12, 13) after passing through July 10
- All failed runs logged the same error:
  ```
  psycopg2.OperationalError: connection to server at "localhost" (::1), port 5432 failed: Connection refused
  connection to server at "localhost" (127.0.0.1), port 5432 failed: FATAL: password authentication failed for user "postgres"
  ```
- The flyctl proxy connected successfully (the IPv4 connection reached the server) but the database rejected the credential
- `flyctl status -a cinderhaven-db` showed the machine running normally (started, primary role)
- Local connection using the `.env` POSTGRES_PASSWORD succeeded immediately

## What Didn't Work

The credential desync is the tail end of a longer saga across prior sessions (session history):

- **2026-06-20**: The `postgres` role password was changed via local-trust-socket `ALTER USER` to fix downstream app access, but no Fly-managed secret was updated at the time. This created the first divergence between the DB-level password and Fly's internal credential tracking.
- **2026-07-01/02**: Three separate attempts to realign Fly-managed secrets (`OPERATOR_PASSWORD`, `SU_PASSWORD`) during debugging of the separate `flypgadmin` pg health check issue. During this work, `OPERATOR_PASSWORD` was modified — this is the change that ultimately caused the GitHub Actions secret to drift when Fly reconciled the `postgres` role password on a later restart.
- **2026-07-10**: While setting `DATABASE_URL` secrets for other Fly apps (`spinrate`, `voidfinder`), the `.env` password was observed to intermittently fail authentication, then self-recover — attributed to stale connection state during a rolling deploy. (session history)

The `flypgadmin` pg health check failure is a separate, proven-unrepairable-in-place issue (documented in auto memory `cinderhaven-db-pg-health-check-blocked`) and does not affect the app-facing `postgres` role or this workflow. (auto memory [claude])

## Solution

Updated the GitHub Actions `POSTGRES_PASSWORD` secret to match the current working password from the local `.env` file, using a credential-safe stdin pipe that keeps the password out of shell history and process arguments:

```bash
grep "^POSTGRES_PASSWORD" cinderhaven-data-platform/.env | cut -d= -f2 \
  | gh secret set POSTGRES_PASSWORD --repo MsShawnP/cinderhaven-data-platform
```

Triggered a manual workflow run to verify:

```bash
gh workflow run canonical-drift.yml --repo MsShawnP/cinderhaven-data-platform
```

The run passed. Only one repo (`cinderhaven-data-platform`) uses `POSTGRES_PASSWORD` in its GitHub Actions workflows.

## Why This Works

Fly Postgres reconciles the `postgres` database role password to the value of the `OPERATOR_PASSWORD` Fly secret on machine restart. The `OPERATOR_PASSWORD` was modified on July 2 during flypgadmin debugging. Between July 10 and July 11, Fly likely restarted Postgres internally, applying the new `OPERATOR_PASSWORD` as the `postgres` role's password. At that point the GitHub Actions secret — set on June 30 from the then-current `.env` — held a stale value.

The credential dependency chain:

```
Fly OPERATOR_PASSWORD secret
  --> postgres role password (reconciled on restart)
    --> local .env POSTGRES_PASSWORD (manual sync)
      --> GitHub Actions POSTGRES_PASSWORD secret (manual sync)
```

When any upstream link changes, all downstream links must be updated manually. There is no automated sync between these surfaces.

## Prevention

1. **After any Fly secret change, update GitHub Actions secrets immediately.** Any time `OPERATOR_PASSWORD` or the postgres role password changes on Fly, run the `grep | gh secret set` pipeline above. A future checklist or runbook step would formalize this.
2. **Use stdin pipes for credential updates.** Always use `grep ... | gh secret set` rather than `gh secret set --body <value>` to keep credentials out of shell history and process argument lists.
3. **Trigger a manual workflow run after credential changes.** Run `gh workflow run canonical-drift.yml` after any secret update to catch mismatches immediately rather than waiting for the next scheduled run.
4. **Monitor the workflow.** The canonical drift check runs daily at 13:00 UTC. A streak of failures indicates either data drift (the intended signal) or credential drift (this failure mode). Check `gh run list --workflow=canonical-drift.yml --limit=5` periodically.

## Related Issues

- `FAILURES.md` (2026-07-02): "flyctl ssh console -C silently corrupts inline secrets" and "cinderhaven-db flypgadmin credential fixes don't survive restart" document the credential management history that led to this drift
- `DECISIONS.md` (2026-07-02): "Stop guessing at cinderhaven-db's flypgadmin credential; rebuild or escalate instead"
- `.github/workflows/canonical-drift.yml`: the workflow affected
- Auto memory `cinderhaven-db-pg-health-check-blocked`: documents the separate flypgadmin issue (cosmetic, does not affect this workflow)
