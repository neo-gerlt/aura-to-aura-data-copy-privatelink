# Aura-to-Aura Data Copy with Private Link

**Goal: securely copy data between two Neo4j Aura instances entirely inside AWS, over PrivateLink, with no traffic crossing the public internet.**

This script is for migrations that meet **all** of the following assumptions:

- **Source is read-only** for the duration of the run (application paused, in maintenance, or otherwise quiescent — see [Source consistency](#source-consistency--assumes-a-quiescent-source) for what goes wrong otherwise).
- **Target is empty** at the start of the run (the script aborts on a non-empty target unless `--overwrite` or `--resume` is passed).
- **Both instances are reachable only via VPC PrivateLink** (public traffic disabled on Aura).
- **Domain identity is preserved via your own properties** (e.g. `User.user_id`); Neo4j-internal `elementId` values are NOT preserved on the target — see [Identity preservation](#identity-preservation--element-ids-are-not-preserved).

This is the alternative to `neo4j-admin database upload`, which is point-in-time consistent and preserves elementIds, but **requires the target's public Bolt to be reachable** during the upload — incompatible with PL-only Aura instances.

How it works: the script connects to both Aura instances simultaneously over **Bolt (port 7687)** — the same protocol PrivateLink proxies — reads from the source, and writes to the target in batches, one phase at a time. See [How it works](#how-it-works) for phase details.

Two modes are supported:

- **`--mode=local`** — run the migration directly from the current machine. Use this when you are already inside the customer VPC (e.g. on a bastion or existing EC2).
- **`--mode=ec2`** — the script provisions a fresh EC2 in the customer's VPC, embeds itself onto the instance via base64 (no GitHub fetch — what runs is exactly the local code), runs the migration there, streams the output back in near-real time, then terminates the instance automatically. Aura passwords are prompted interactively and stored temporarily in SSM Parameter Store as `SecureString`. They are never written to disk, never passed via user-data, and never appear in process argv on the EC2 — they are pulled from SSM into env vars right before invoking the migration.

  **Real-time streaming.** SSM RunCommand buffers stdout for long-running commands and only releases it when the command terminates — for a 30+ minute migration that would mean a blank screen until the end. To work around this, the migration on the EC2 tees its output to `/tmp/migrate.log`, and the local script polls that file via short sidecar `tail -c +N` SSM commands every few seconds. Sidecar commands return immediately, so output reaches the local terminal in near-real time.

  **AL2023 bootstrap.** The migration EC2 uses the Amazon Linux 2023 latest AMI, which ships Python 3 but not pip. The bootstrap script installs `python3-pip` via `dnf` before invoking pip, so no AMI pre-baking is required.

  **SSM 12-hour timeout.** The SSM `AWS-RunShellScript` document has two separate timeouts: the API-level `TimeoutSeconds` and the document-level `executionTimeout`. The smaller one wins; if `executionTimeout` is left at its default the migration is SIGKILL'd at 1 hour regardless of API-level setting. The script sets both to 12 hours (43,200 s), comfortably above the ~75-minute runtime observed for a 4.7M-node / 10M-rel dataset over PL. For larger datasets, raise both values in the script (max for SSM is 48 hours).

---

## Requirements

- Python 3.9+
- `pip install -r requirements.txt`
- **Local mode:** the machine must have Bolt (port 7687) access to both Aura instances
- **EC2 mode:** AWS credentials with permissions to launch EC2, write SSM parameters, and send SSM commands; `boto3` is required (`pip install boto3`)

---

## Usage

### Local mode

```bash
export SOURCE_AURA_URI=neo4j+s://xxxx.databases.neo4j.io
export SOURCE_AURA_PASSWORD=secret
export TARGET_AURA_URI=neo4j+s://yyyy.databases.neo4j.io
export TARGET_AURA_PASSWORD=secret

# Test connectivity first
python3 aura_to_aura_migration.py --mode=local --dry-run

# Full migration
python3 aura_to_aura_migration.py --mode=local

# Resume after interruption
python3 aura_to_aura_migration.py --mode=local --resume
```

### EC2 mode

```bash
python3 aura_to_aura_migration.py --mode=ec2 \
  --source-uri=neo4j+s://xxxx.databases.neo4j.io \
  --target-uri=neo4j+s://yyyy.databases.neo4j.io \
  --subnet-id=subnet-0abc1234 \
  --security-group-id=sg-0abc1234 \
  --instance-profile=arn:aws:iam::123456789012:instance-profile/aura-migration
```

Aura passwords are prompted interactively.

**Watching progress.** When you run the script in the foreground (the typical interactive case), Phase banners and per-label/per-rel-type heartbeat lines stream directly to your terminal in near-real time — no extra command needed:

```
[User] 2,427,349/4,678,779 (51.9%) rate 5,324/s elapsed 456s ETA 423s
```

If you'd rather detach the run (e.g. close your laptop and check back later), redirect output to a log file and tail it from another shell:

```bash
nohup python3 aura_to_aura_migration.py --mode=ec2 ... \
  > migration.log 2>&1 &

# from anywhere — same machine, same user:
tail -f migration.log
```

**Don't** kill the local script (Ctrl-C / closing the terminal) while the migration is running, even though the EC2 keeps going. The local script also handles cleanup in its `finally` block: terminating the migration EC2 and deleting the SSM password parameters. If you kill the local script ungracefully, the EC2 will keep running until the migration completes and shutdown-on-terminate fires, but the SSM parameters will linger and you'll be paying for compute longer than necessary. If you must detach, use `nohup ... &` *before* the script starts so it owns its own session.

**Required IAM:**

- The **invoking machine** (where you run `--mode=ec2`) needs `ec2:RunInstances`, `ec2:TerminateInstances`, `ec2:DescribeInstances`, `ssm:PutParameter` + `ssm:DeleteParameter` on `/aura-migration/*`, `ssm:SendCommand`, and `ssm:GetCommandInvocation`.
- The **instance profile** (used by the migration EC2 itself) needs only `ssm:GetParameter` on `/aura-migration/*` and the AWS-managed `AmazonSSMManagedInstanceCore` policy so the SSM agent can register and execute commands.

All connection parameters can also be passed as CLI flags — run `--help` for the full list.

### Multi-PrivateLink deployments

The script doesn't care whether source and target share a single PrivateLink endpoint or live behind two different ones — it just opens Bolt connections to two URIs and the customer VPC's networking handles the rest.

**Same Aura orch cluster (single-PL setup).** When both Aura instances are in the same orch cluster (their Bolt URIs share the same `production-orch-XXXX.neo4j.io` parent), one VPC endpoint covers both with one Private DNS zone (`*.production-orch-XXXX.neo4j.io`) and one TLS cert.

**Different Aura orch clusters (multi-PL setup).** When source and target are in different orch clusters, Aura returns two different endpoint service names. Customer creates two VPC endpoints in the same VPC — one per service — and each endpoint registers its own Private DNS zone and presents its own TLS cert. The migration command is unchanged in shape:

```bash
python3 aura_to_aura_migration.py --mode=ec2 \
  --source-uri=neo4j+s://aaaaaaaa.production-orch-0435.neo4j.io \
  --target-uri=neo4j+s://bbbbbbbb.production-orch-0789.neo4j.io \
  ...
```

The migration EC2's outbound SG (default allow all) reaches both endpoint ENIs. Each endpoint can use its own SG or share one — inbound rules just need to permit `tcp/80,443,7687` from the VPC CIDR.

Cost note: each Interface VPC endpoint runs ~$0.014/hr (~$10/mo idle) plus per-GB transit. Clean both up after the migration completes.

---

## How it works

### Phase 0 — Pre-flight
Verifies connectivity to both instances, counts nodes and relationships on the source, and checks whether the target is empty. Exits if the target has data and you haven't passed `--overwrite`.

### Phase 1 — Schema
Runs `SHOW CONSTRAINTS` and `SHOW INDEXES` on the source, then replays those DDL statements on the target. This runs first so constraints are enforced as data arrives. LOOKUP indexes are skipped (Neo4j auto-creates them); all other index types — RANGE, TEXT, POINT, VECTOR, and FULLTEXT — are migrated.

### Phase 2 — Nodes
For each node label, pages through all nodes in batches of 5,000 using an **elementId cursor** (`WHERE elementId(n) > $last_eid ORDER BY elementId(n) LIMIT batch_size`). This avoids the O(n²) cost of `SKIP/LIMIT` on large labels. Nodes are grouped by their **full label combination** (a node with `:Person:Employee` labels is created with both labels in one `CREATE` — no APOC required). Each batch is written to the target with `UNWIND ... CREATE`, and the mapping of `source elementId → target elementId` is stored in a SQLite database on disk.

The SQLite map handles multi-label nodes correctly: if a node has labels `[:Person, :Employee]`, it appears in both the `Person` pass and the `Employee` pass. The second time, the script sees it's already mapped and skips it.

### Phase 3 — Relationships
For each relationship type, pages through source relationships in batches using the same elementId cursor pattern. For each batch, resolves start and end node target IDs from SQLite, then creates the relationships on the target using those IDs.

### Phase 4 — Validation
Counts nodes per label and relationships per type on both instances, and checks that all non-LOOKUP indexes from the source are present on the target. Prints a pass/fail table for each. Exits with code `2` if any counts or indexes mismatch.

---

## Source consistency — assumes a quiescent source

**The source database must be effectively read-only for the duration of the migration.** The script does not snapshot the source, does not hold a long-running read transaction, and does not lock the source. Each batch is its own auto-commit read transaction, so every batch sees whatever source state exists at the moment it runs.

This is intentional — a multi-hour read transaction would block source writes anyway, would be killed by Aura session timeouts, and would make `--resume` impossible. The tradeoff is that the script assumes the customer's application is paused (or routed elsewhere) during the migration window.

### What goes wrong if the source is being written to

The script paginates with an `elementId` cursor (`WHERE elementId(n) > $last_eid ORDER BY elementId(n)`). Whether a concurrent write is captured or missed depends on where the cursor is when the write commits:

| Source change during migration | Result |
|---|---|
| New node, written before cursor reaches its label/elementId | ✓ copied to target |
| New node, written after cursor passed its label | ✗ missed; absent from target |
| Existing node modified before cursor reaches it | ✓ target reflects modified properties |
| Existing node modified after cursor copied it | ✗ target reflects stale (pre-modification) properties |
| Existing node deleted before cursor reaches it | ✓ correctly absent from target |
| Existing node deleted after cursor copied it | ✗ target retains deleted node — diverges from source |
| New relationship between two new nodes (both created after their label cursors passed) | ✗ start/end nodes missing from SQLite ID map; script logs `WARN: N relationships skipped (node not in ID map)` |
| Schema change on source after Phase 1 | ✗ never replayed; target schema diverges |

### How you'll know

Phase 4 (Validation) catches this at the end of the run:

```
── Phase 4: Validation ──────────────────────────────────────────
  Label                                    Source       Target   OK?
  ----------------------------------- ------------ ------------ -----
  User                                  4,675,012    4,674,239     ✗   ← 773 missed
  ...
  Result: FAILED ✗
```

You'll also see `WARN: N relationships skipped (node not in ID map)` lines in Phase 3 if relationships were written between nodes that hadn't been migrated yet. So a divergent migration won't go unnoticed — but it does mean the target isn't usable as a clean cutover until the divergence is resolved.

### Mitigations for live source migrations

If pausing source writes isn't acceptable, options include:

- **Maintenance window cutover.** Stop application writes, run the migration, switch app over to the new instance, accept the downtime.
- **Aura-native dump/restore** if your network model allows public connectivity — Aura's dump is point-in-time consistent.
- **Migration + CDC replay.** Run this script with the source live, then replay any deltas captured by CDC tooling between Phase 0 start and target cutover. (Out of scope for this script.)
- **Application-level dual-write with cutover.** App writes to both instances during a transition window; cut over reads when target is verified consistent. (Out of scope for this script.)

---

## Identity preservation — element IDs are NOT preserved

Unlike `neo4j-admin database upload` or a backup/restore workflow (which copy raw store files and preserve every internal identifier), this script **creates new nodes and relationships on the target** with the source data's properties, labels, and types. The target's Neo4j assigns its own `elementId` values to each new node and relationship.

| | Backup / restore | This script |
|---|---|---|
| `elementId(n)` of a node on source vs target | Identical | Different |
| `elementId(r)` of a relationship | Identical | Different |
| Internal numeric `id(n)` (deprecated) | Identical | Different |
| Domain properties (`User.user_id`, `User.name`, etc.) | Preserved | Preserved |
| Labels and relationship types | Preserved | Preserved |
| Indexes and constraints | Preserved | Replayed (see Phase 1) |

**What breaks if downstream systems reference Neo4j-internal IDs:**

- External application code that stores `elementId` (or the deprecated numeric `id()`) as a foreign key in another system — those references won't resolve on the target.
- Cypher queries that hard-code an `elementId` literal — won't find their node on the new instance.
- Audit logs that include `elementId` — still readable, but no longer point to the same physical node.

**What works fine:**

- Anything that references nodes/rels by domain properties (`MATCH (u:User {user_id: 12345})`) — the property-based identity moves through the migration intact.

The script's internal SQLite ID map (`migration_ids.db`) preserves `source_elementId → target_elementId` mappings *during the run* so the relationship phase can resolve endpoints. It's not intended as a long-term lookup table and is safe to delete once validation passes.

If preserving Neo4j-internal IDs matters for your use case (e.g., the application encodes `elementId` in URLs or external references), this script is the wrong tool — use `neo4j-admin database dump` + `neo4j-admin database upload` instead, accepting the requirement that public traffic is enabled on the target during the upload.

---

## Resume / crash recovery

After every batch the script writes a **checkpoint JSON file** tracking which labels and relationship types are complete and the highest source `elementId` written so far. If the script is interrupted, re-run with `--resume` to continue from exactly where it left off. The SQLite ID map persists across restarts so the relationship phase can proceed after a node-phase crash.

**Where the checkpoint and ID map live.** Both files default to the **current working directory** (`migration_checkpoint.json` and `migration_ids.db`). This means a `--resume` only works if you re-launch from the *same directory* that ran the original migration. If you need stable absolute paths (e.g. resuming from a different shell, a CI runner, or a different user's session), pass `--checkpoint-file=/abs/path.json` and `--id-map-db=/abs/path.db` on both runs.

Transient connection errors (dropped Bolt connections, temporary Aura unavailability) are retried automatically up to 3 times with exponential backoff (2s, 5s, 15s) before the script fails. Most short network blips are handled transparently without needing a manual `--resume`.

The checkpoint format is versioned. If the format changes incompatibly, `--resume` will refuse to load an old checkpoint and tell you to delete the JSON + ID map and start fresh.

### Caveat: duplicate relationships on resume after a hard crash

If the process dies in the small window **between** a relationship batch's write commit and the corresponding checkpoint save, `--resume` will re-run that batch and create duplicate relationships on the target (Neo4j allows multiple parallel rels of the same type between the same nodes). The window is sub-second, but it exists. If validation reports a target relationship count higher than the source, the safest fix is to drop the target database and re-run from scratch. This does not affect the node phase (the SQLite ID map dedups multi-label visits and post-crash retries by source elementId).

---

## Key flags

| Flag | Description |
|---|---|
| `--mode` | `local` (default) or `ec2` |
| `--dry-run` | Connect and count source only; no writes |
| `--resume` | Continue from last checkpoint |
| `--overwrite` | Proceed into a non-empty target |
| `--skip-schema` | Skip constraint/index migration |
| `--skip-validation` | Skip post-migration count comparison |
| `--batch-size N` | With auto-tune ON (default), the **initial bootstrap value** when explicitly set; the loop adapts from there. With auto-tune OFF, a **fixed batch size**. (default: 10,000) |
| `--auto-tune-batch-size` / `--no-auto-tune-batch-size` | Adapt batch size at runtime based on per-batch wall-time, OOMs, and throughput plateau. Default: ON. Disable for benchmarks where reproducibility matters more than throughput. See [Adaptive Batch Sizing](#adaptive-batch-sizing). |

**EC2 mode flags** (required when `--mode=ec2`):

| Flag | Env var | Description |
|---|---|---|
| `--subnet-id` | `AWS_SUBNET_ID` | Subnet for the migration EC2 |
| `--security-group-id` | `AWS_SECURITY_GROUP_ID` | Must allow outbound port 7687 |
| `--instance-profile` | `AWS_INSTANCE_PROFILE` | IAM instance profile ARN |
| `--instance-type` | — | EC2 instance type (default: `t3.medium`) |
| `--aws-region` | `AWS_DEFAULT_REGION` | AWS region |

---

## Adaptive Batch Sizing

When `--auto-tune-batch-size` is on (the default), the script picks a per-label / per-rel-type batch size at runtime instead of using a single global `--batch-size`. Same code, no flag changes — runs on a 4 GB Aura tier and a 64 GB tier and discovers the right ceiling for each from observed signals.

### What it does

Two-level design:

1. **Bootstrap pick (initial batch only)** — based on the label / rel-type's total row count from Phase 0:

   | Total rows | Initial batch |
   |---|---|
   | < 5,000 | total (one shot) |
   | < 100,000 | 5,000 |
   | < 1,000,000 | 10,000 |
   | ≥ 1,000,000 | 20,000 |

   This is a starting hint only — from batch 2 onward, the adaptive loop is the source of truth. An explicit `--batch-size N` overrides the bootstrap.

2. **Adaptive loop** — three runtime signals drive it:

   - **Wall-time slow/fast.** A batch is "slow" if it took > 5 s, "fast" if < 1 s. After 3 consecutive slow batches → halve. After 5 consecutive fast batches → double.
   - **`MemoryPoolOutOfMemoryError`** — sticky hard ceiling. The OOMing size halves and locks: doubling can never bring the loop back to a size that's known to OOM. Sub-divides the failed batch and retries at the new size; consecutive OOMs at retry are not coalesced — each is a real signal.
   - **Throughput plateau.** A double must improve rows/sec by ≥ 10% to count as progress. If it doesn't, doubling stops for the rest of the label/rel-type — the bottleneck has shifted from batch overhead to something else (network, target write throughput, driver serialization).

   The loop also tracks a **soft ceiling** (the highest size that has produced fast batches) so a transient slow window can halve, then climb back up to known-good without going through the doubling-growth path. This is what makes the algorithm robust against single GC pauses without permanent regression.

### What you'll see

Heartbeat lines (every 20 s, same cadence as today) gain four auto-tune fields:

```
[CHATTER] 8,450,000/10,018,281 (84.4%) rate 5,890/s elapsed 1,434s ETA 266s last_batch_rate=12,300/s batch=20,000 ceiling=40,000 soft_ceiling=20,000
```

| Field | Meaning |
|---|---|
| `rate` | **Cumulative** rows/sec since the label/rel-type started (unchanged from before) |
| `last_batch_rate` | Rows/sec of the most recent completed batch (auto-tune's per-batch view) |
| `batch` | Current batch size the loop has settled on |
| `ceiling` | Hard upper bound (`∞` if never tripped); set by OOM or repeated steady-state slow |
| `soft_ceiling` | Highest size that has produced fast batches; if `batch < soft_ceiling`, the loop is in slow-recovery |

State-transition events (halve, double, OOM, plateau-lock) emit their own stderr WARN lines so transitions are easy to grep for in long-run logs.

### Failure modes

If the target instance can't sustain even `MIN_BATCH = 1,000` rows for some label, the script raises `InstanceTooSmall`, prints an operator-friendly message, preserves the checkpoint, and exits with code **3** (distinct from generic exit 1, so EC2-mode wrappers can recognize "this is a tier mismatch, not a transient"). Re-run with `--resume` after upgrading the target Aura tier.

### When to disable

Pass `--no-auto-tune-batch-size` to disable. The static path is byte-identical to pre-auto-tune behavior. Use it for:

- **Benchmark reproducibility** — fixed batches are required to compare runs.
- **Diagnosing whether auto-tune is implicated** in a regression.
- **Tier-cap regression tests** — pinning a known-too-large batch to confirm the OOM repro on a tier.

### Resume behavior

Auto-tune state lives in memory only — it is NOT included in the checkpoint JSON. After `--resume`, the loop bootstraps fresh and re-discovers ceilings on the first few batches per label/rel-type. The cost is one OOM + halve per type that previously hit OOM; on a multi-million-row label this is seconds, not minutes. The banner prints a one-liner reminder when `--resume` is used.

---

## Performance

Numbers from a real Aura-to-Aura run on a 4.68M-node / 10.08M-rel dataset (no Private Link, both instances in the same AWS region):

- **Phase 2 (nodes):** ~3,000–3,700 nodes/sec sustained at the default batch size.
- **Phase 3 (relationships):** ~800–1,100 rels/sec at `--batch-size 5000`; ~3,000 rels/sec at `--batch-size 20000` early on. Each relationship write is ~3× the per-row cost of a node write because it does two `MATCH`-by-elementId lookups plus a `CREATE` per rel; bigger batches amortize the per-transaction round-trip. The default of `10,000` is a balance between throughput and per-transaction load.

### PrivateLink-only Aura-to-Aura (`--mode=ec2`) — empirical numbers

Numbers from a 4.68M-node / 10.08M-rel migration run with both Aura instances reachable only via PL. **Both instances were 4 GB Aura tier (small)** — these numbers are the floor for this script, not the ceiling. Bigger Aura tiers (16-128 GB Pro/Business) have more CPU, more memory, and a higher transaction-memory cap, all of which raise both the practical batch-size ceiling and the per-batch throughput.

- **Phase 2 (nodes):** ~5,300 nodes/sec at `--batch-size 10000`; ~6,200 nodes/sec at `--batch-size 40000` (~17% gain). Phase 2 is mostly bound by Aura target's commit speed.
- **Phase 3 (relationships):** ~1,580 rels/sec at `--batch-size 10000`; ~2,500 rels/sec at `--batch-size 20000` (~58% gain).

### 32 GB / 32 GB tier with `--auto-tune-batch-size` (default) — empirical numbers

Numbers from the same 4.68M-node / 10.08M-rel dataset on **32 GB Aura tier** for both source and target, auto-tune ON (default), no other flags. Captured 2026-05-03.

- **Total wall time: 47.6 min** (vs 75 min on 4 GB tier — ~37% faster).
- **Phase 2 (nodes):** ~6,181 nodes/sec cumulative on the User label (4.68M rows). Auto-tune held the 20K bootstrap pick — write batches landed at ~1.5 s wall time, between `FAST_SEC=1.0` and `SLOW_SEC=5.0`, so doubling never fired. Phase 2 looks **source-read bound** on bigger tiers; the 32 GB target's extra commit headroom is unused at this batch size.
- **Phase 3 (relationships):** ~4,881 rels/sec cumulative on CHATTER (10M rows) — roughly **2× the 4 GB tier at the same flag set**. Auto-tune **doubled CHATTER from 20K → 40K** after the first ~50 fast batches (the same size that triggered `MemoryPoolOutOfMemoryError` on a 4 GB target). `ceiling=∞` held all the way through; no OOM, no slow-halve, no plateau lock.
- **Late-CHATTER pace:** the 4 GB tier's documented degradation from ~3,000 → ~50 rels/sec in the final 5–10% of CHATTER did **not** reproduce on 32 GB. Per-batch rates stayed in the 18,000–21,000 rels/sec band through 99.0%, with one transient blip at 16,010 rels/sec that recovered in the next batch. The slow-halve mechanism never engaged because the 32 GB source had ample headroom.
- **Phase 4 validation:** PASSED — all 5 labels and 6 rel types matched source = target counts; all 5 RANGE constraints present.

**What this validates and what it doesn't.** Validates: auto-tune correctness end-to-end, OOM-sticky-ceiling never triggering false positives, the static→adaptive transition at the 4 GB→32 GB boundary, the 40K-batch path that previously OOM'd. Does NOT validate: OOM-driven ceiling lowering (no OOM occurred on this tier), steady-state slow detection (no slow window ≥ SLOW_SEC=5.0), the recovery path after a slow halve, plateau lock at 80K (loop never reached doubling-to-80K because 40K-row batches landed at ~2 s, between FAST_SEC and SLOW_SEC). Those code paths still need a load test that exercises them.

**Implementation gap surfaced.** With `FAST_SEC=1.0`, the loop's doubling check stops firing once batches reach the ~1–2 s wall-time band. On 32 GB this means we get exactly one doubling step (20K → 40K) and stall. A relaxed `FAST_SEC=2.0` or `~3.0` would let the loop probe further into the headroom 32 GB+ tiers offer. Tracked alongside the broader Phase 2/3 stage profiling in `tsk-20260503-aura-migration-perf-profile`.

**Source CPU is the Phase 3 bottleneck for large rel types — even more so on small tiers.** During CHATTER (10M rels) at `--batch-size 20000` on a 4 GB source, observed source Aura CPU at ~100% while target Aura CPU was at ~10-30%. Per-batch wall time was ~7.9 s, of which ~6.6 s was the source's elementId-paginated read query (`MATCH ()-[r:type]->() WHERE elementId(r) > $last_eid ORDER BY elementId(r) LIMIT batch_size`). The remaining ~1.3 s went to SQLite ID-map lookup + Bolt round-trip + target write + checkpoint save. So bigger batches help only modestly past 20K — the source read can't go faster on a single thread on a small instance.

**Single-session source reads also can't scale across the cluster.** Aura is always a multi-node cluster (typically 3 members), but the script's sequential reads come from one driver session, which Bolt routing pins to one cluster member. The `100%` source CPU you'd observe is one cluster node pegged while the other two are idle for our workload. The `default_access_mode` hint is also not set today, so the session may be classified as write-capable and pinned to the cluster leader (most-contended node) rather than a follower. Both limitations get addressed together by the parallel-relationship-workers feature (`tsk-20260502-aura-migration-parallel-rels`): N concurrent workers, each with its own source session in `READ` mode, gives Bolt routing a chance to spread reads across cluster followers.

**Real levers for higher Phase 3 throughput:**

- **Source Aura tier upgrade** (more CPU/memory) directly raises the per-query throughput ceiling. Going from 4 GB to 16+ GB will likely 2-4× the rels/sec by itself.
- **Parallel relationship workers** (range-partition the elementId space across N concurrent readers) — see follow-up task `tsk-20260502-aura-migration-parallel-rels`. Expected ~1.5–2× throughput from N=2 since both source and target have headroom for concurrent transactions.
- **Adaptive per-rel-type batch sizing** — implemented; on by default (`--auto-tune-batch-size`). The script picks per-rel-type batch sizes at runtime and discovers the target's tx-memory ceiling from observed `MemoryPoolOutOfMemoryError` rather than relying on operator guesswork. See [Adaptive Batch Sizing](#adaptive-batch-sizing) above.

**`--batch-size` ceiling on small tiers:** observed `MemoryPoolOutOfMemoryError` at `--batch-size 40000` for CHATTER on a 4 GB target's 1.3 GiB tx-memory cap. Each row's `MATCH (a) WHERE elementId(a) = ... MATCH (b) WHERE elementId(b) = ... CREATE (a)-[r]->(b)` retains node references in transaction memory; 40,000 × ~33 KB/row ≈ 1.3 GiB. With auto-tune ON (default), the script discovers this ceiling at runtime — on a 4 GB target it will OOM once at 40K, lock the ceiling at 20K, and run cleanly thereafter. With auto-tune OFF, **`--batch-size 10000` stays safe across all tiers** and is the recommended fixed value when reproducibility matters more than throughput.

**Pace degradation in long Phase 3 runs.** In the same test, the rate started at ~3,000 rels/sec and degraded to ~50 rels/sec in the final 5–10% of CHATTER (the largest rel type), eventually triggering sustained `SessionExpired` events on the source bolt session. This is consistent with the source Aura instance being under sustained read pressure. Mitigations the script already applies:

- Cursor-based pagination (no `SKIP/LIMIT`), so source-side cost stays linear.
- Retry budget of 6 attempts with exponential backoff up to 2 minutes (~3.5 min total window) on `TransientError`, `ServiceUnavailable`, `SessionExpired`, and `DatabaseUnavailable`.
- Per-batch checkpointing so `--resume` always picks up cleanly.

If you observe sustained slowdowns in Phase 3, dropping `--batch-size` (e.g. to 5,000) reduces per-transaction load on the source and often recovers the rate. Resume with the smaller batch size — the cursor position is preserved.

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
  Batch  : auto-tune ON (bootstrap from per-label counts)
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

  [User] 109,900/4,674,239 (2.4%) rate 5,172/s elapsed 21s ETA 883s
  [User] 219,795/4,674,239 (4.7%) rate 5,217/s elapsed 42s ETA 855s
  [User] 988,964/4,674,239 (21.2%) rate 5,253/s elapsed 188s ETA 702s
  ...
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
