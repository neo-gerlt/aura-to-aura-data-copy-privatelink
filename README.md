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
Runs `SHOW CONSTRAINTS` and `SHOW INDEXES` on the source, then replays those DDL statements on the target. This runs first so constraints are enforced as data arrives. LOOKUP indexes are skipped (Neo4j auto-creates them); all other index types — RANGE, TEXT, POINT, VECTOR, and FULLTEXT — are migrated.

### Phase 2 — Nodes
For each node label, pages through all nodes in batches of 5,000 using `SKIP/LIMIT`. Nodes are grouped by their **full label combination** (a node with `:Person:Employee` labels is created with both labels in one `CREATE` — no APOC required). Each batch is written to the target with `UNWIND ... CREATE`, and the mapping of `source elementId → target elementId` is stored in a SQLite database on disk.

The SQLite map handles multi-label nodes correctly: if a node has labels `[:Person, :Employee]`, it appears in both the `Person` pass and the `Employee` pass. The second time, the script sees it's already mapped and skips it.

### Phase 3 — Relationships
For each relationship type, pages through source relationships in batches. For each batch, resolves start and end node target IDs from SQLite, then creates the relationships on the target using those IDs.

### Phase 4 — Validation
Counts nodes per label and relationships per type on both instances, and checks that all non-LOOKUP indexes from the source are present on the target. Prints a pass/fail table for each. Exits with code `2` if any counts or indexes mismatch.

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

---

## Sample output

```
══════════════════════════════════════════════════════════════
  Aura → Aura Migration
  Source : neo4j+s://xxxx.databases.neo4j.io
  Target : neo4j+s://yyyy.databases.neo4j.io
  Mode   : LIVE
  Batch  : 5,000 rows/tx
══════════════════════════════════════════════════════════════

── Connecting ───────────────────────────────────────────────────
  ✓ Connected to source: neo4j+s://xxxx.databases.neo4j.io
  ✓ Connected to target: neo4j+s://yyyy.databases.neo4j.io

── Phase 0: Pre-flight ──────────────────────────────────────────
  Source: 4,680,870 nodes, 10,074,938 relationships
  Target: 0 nodes currently

── Phase 1: Schema ──────────────────────────────────────────────
  Constraints: 5
  Indexes:     5
  Schema done.

── Phase 2: Nodes ───────────────────────────────────────────────
  Labels: User, Stream, Team, Game, Language

  [User] 4,674,239 new nodes (label total on source: 4,674,239)
  [Stream] 4,540 new nodes (label total on source: 4,540)
  [Team] 1,468 new nodes (label total on source: 1,468)
  [Game] 594 new nodes (label total on source: 594)
  [Language] 29 new nodes (label total on source: 29)

  Node migration complete: 4,680,870 nodes written

── Phase 3: Relationships ───────────────────────────────────────
  Rel types: CHATTER, MODERATOR, VIP, PLAYS, HAS_LANGUAGE, HAS_TEAM

  [CHATTER] 10,018,281 relationships written (source total: 10,018,281)
  [MODERATOR] 29,746 relationships written (source total: 29,746)
  [VIP] 15,694 relationships written (source total: 15,694)
  [PLAYS] 5,696 relationships written (source total: 5,696)
  [HAS_LANGUAGE] 4,541 relationships written (source total: 4,541)
  [HAS_TEAM] 2,980 relationships written (source total: 2,980)

  Relationship migration complete: 10,076,938 relationships written

── Phase 4: Validation ──────────────────────────────────────────

  Label                                    Source       Target   OK?
  ----------------------------------- ------------ ------------ -----
  Game                                        594          594     ✓
  Language                                     29           29     ✓
  Stream                                    4,540        4,540     ✓
  Team                                      1,468        1,468     ✓
  User                                  4,674,239    4,674,239     ✓

  Rel Type                                 Source       Target   OK?
  ----------------------------------- ------------ ------------ -----
  CHATTER                              10,018,281   10,018,281     ✓
  HAS_LANGUAGE                              4,541        4,541     ✓
  HAS_TEAM                                  2,980        2,980     ✓
  MODERATOR                                29,746       29,746     ✓
  PLAYS                                     5,696        5,696     ✓
  VIP                                      15,694       15,694     ✓

  Index                                Type           OK?
  ----------------------------------- ------------ -----
  constraint_2b4f13ce                 RANGE            ✓
  constraint_5018efe5                 RANGE            ✓
  constraint_5101b27d                 RANGE            ✓
  constraint_952bbd70                 RANGE            ✓
  constraint_d7191a75                 RANGE            ✓

  Result: PASSED ✓

══════════════════════════════════════════════════════════════
  Done in 47.3 min
  Checkpoint : migration_checkpoint.json
  ID map     : migration_ids.db
══════════════════════════════════════════════════════════════
```
