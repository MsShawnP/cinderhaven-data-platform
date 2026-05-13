# Cinderhaven Data Platform — Failure Log

What was attempted that didn't work, why it didn't work, and what was
tried next.

Lower bar than DECISIONS.md — capture failures even when they didn't
produce a durable rule. The whole point: future-you (or future-Claude)
shouldn't re-attempt dead ends because the lesson got lost.

---

## Format

### YYYY-MM-DD — [One-line failure description]

**Attempted:** [What was tried]

**Why it didn't work:** [Concrete reason, not "it broke." If the
failure mode was technical, name the specific issue. If the failure
mode was scope or approach, name that.]

**What we tried instead:** [The next attempt, which may also have
failed and may have its own entry below]

**Status:** Resolved / open / abandoned

**Tags:** [keywords for future text-search]

---

## Entries

### 2026-05-12 — Fly.io shared-cpu-1x (256MB) crashes during scan_data ingestion

**Attempted:** Load 1.1M-row scan_data table into Postgres via proxy tunnel.
Tried three approaches at 256MB: (1) execute_batch with 5000-row batches,
(2) COPY with 50,000-row chunks, (3) COPY with 5,000-row chunks and
per-chunk reconnection. All three crashed the Postgres process — "server
closed the connection unexpectedly."

**Why it didn't work:** The 256MB shared instance runs out of memory during
sustained write operations. The COPY command, WAL writes, and transaction
log together exceed available RAM on scan_data's volume. Smaller tables
(up to 30k rows) load fine at 256MB.

**What we tried instead:** Scaled machine to 1GB temporarily
(`flyctl machine update --memory 1024`), loaded with 25k-row COPY chunks,
scaled back to 256MB. Completed in ~4.5 minutes for all 21 tables.

**Status:** Resolved

**Tags:** fly.io, postgres, memory, ingestion, scan_data, COPY, OOM
