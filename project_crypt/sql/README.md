# Project Crypt SQL Repository

This directory is the durable SQL reference point for Project Crypt.

## Structure

- `migrations/` → schema migration scripts
- `playbooks/` → operational SQL checks, diagnostics, and interpretation

## Current files

- `migrations/2026-02-23_phase2_reliability_bridge_alpha.sql`
  - Adds schema needed for reliability/bridge-alpha wave.

- `playbooks/PHASE2_SQL_QUERIES.md`
  - Query-by-query runbook with:
    - exact SQL
    - what it checks
    - how to interpret results
    - healthy vs red-flag output

## Usage

```bash
sqlite3 /home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3
```

Then copy queries from the playbook.
