# Aura-to-Aura Data Copy with Private Link

Migrate data between two Neo4j Aura instances over Bolt — works with **Private Link enabled and public traffic disabled**.

Instead of `neo4j-admin database upload` (which requires public traffic), this script connects to both Aura instances simultaneously over **Bolt (port 7687)** — the same protocol Private Link proxies. It reads from the source and writes to the target in batches, one phase at a time.

Run from an EC2 inside the customer's VPC that has Private Link endpoints for both source and target Aura instances.

---

## Requirements

- Python 3.9+
- Both Aura instances reachable over Bolt from the machine running the script
- `pip install -r requirements.txt`

---

## Usage

```bash
export SOURCE_AURA_URI=neo4j+s://xxxx.databases.neo4j.io
export SOURCE_AURA_PASSWORD=secret
export TARGET_AURA_URI=neo4j+s://yyyy.databases.neo4j.io
export TARGET_AURA_PASSWORD=secret

# Test connectivity first
python3 aura_to_aura_migration.py --dry-run

# Full migration
python3 aura_to_aura_migration.py

# Resume after interruption
python3 aura_to_aura_migration.py --resume
```

All connection parameters can also be passed as CLI flags — run `--help` for the full list.

---

## How it works

### Phase 0 — Pre-flight
Verifies connectivity to both instances, counts nodes and relationships on the source, and checks whether the target is empty. Exits if the target has data and you haven't passed `--overwrite`.

### Phase 1 — Schema
Runs `SHOW CONSTRAINTS` and `SHOW INDEXES` on the source, then replays those DDL statements on the target. This runs first so constraints are enforced as data arrives.

### Phase 2 — Nodes
For each node label, pages through all nodes in batches of 5,000 using `SKIP/LIMIT`. Nodes are grouped by their **full label combination** (a node with `:Person:Employee` labels is created with both labels in one `CREATE` — no APOC required). Each batch is written to the target with `UNWIND ... CREATE`, and the mapping of `source elementId → target elementId` is stored in a SQLite database on disk.

The SQLite map handles multi-label nodes correctly: if a node has labels `[:Person, :Employee]`, it appears in both the `Person` pass and the `Employee` pass. The second time, the script sees it's already mapped and skips it.

### Phase 3 — Relationships
For each relationship type, pages through source relationships in batches. For each batch, resolves start and end node target IDs from SQLite, then creates the relationships on the target using those IDs.

### Phase 4 — Validation
Counts nodes per label and relationships per type on both instances and prints a pass/fail table. Exits with code `2` if any counts mismatch.

---

## Resume / crash recovery

After every batch the script writes a **checkpoint JSON file** tracking which labels and relationship types are complete and the current `SKIP` offset. If the script is interrupted, re-run with `--resume` to continue from exactly where it left off. The SQLite ID map persists across restarts so the relationship phase can proceed after a node-phase crash.

---

## Key flags

| Flag | Description |
|---|---|
| `--dry-run` | Connect and count source only; no writes |
| `--resume` | Continue from last checkpoint |
| `--overwrite` | Proceed into a non-empty target |
| `--skip-schema` | Skip constraint/index migration |
| `--skip-validation` | Skip post-migration count comparison |
| `--batch-size N` | Rows per transaction (default: 5,000) |

---

## Artifacts

After a successful run, the working directory will contain `migration_checkpoint.json` and `migration_ids.db`. Both are safe to delete once validation passes and are excluded from git via `.gitignore`.
