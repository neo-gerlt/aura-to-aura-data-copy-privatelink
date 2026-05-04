# Aura-to-Aura Data Copy — Detailed Reference (AWS)

> **Scope: AWS only.** Every cloud-orchestration path in this script targets
> AWS specifically — `boto3` for EC2 launch/terminate, AWS Systems Manager
> for command execution and password storage, AWS PrivateLink for Bolt
> connectivity, AWS IAM for the instance profile. There is no GCP or Azure
> equivalent in the current implementation. `--mode=local` will work from
> any host that has direct Bolt reachability to both Aura instances
> (regardless of cloud), but the EC2-orchestration scaffolding documented
> here is AWS-specific.

This document is the deeper companion to [README.md](README.md). The landing
README covers what the script is, how to run it, EC2 sizing, and how to read
profile output. This document covers the operational detail: behavior under
edge cases, every CLI flag, the algorithms behind adaptive batching and
parallel relationships, the in-memory id_map design, deadlock contention
notes, source-consistency caveats, identity preservation tradeoffs, and
sample output.

---

## Use-when, and not-when

The script is appropriate when **all** of these hold:

- **Source is read-only** for the duration of the run. Application paused, in
  maintenance, or otherwise quiescent. See [Source consistency](#source-consistency--assumes-a-quiescent-source)
  for what goes wrong otherwise.
- **Target is empty** at run start. The script aborts on a non-empty target
  unless `--overwrite` or `--resume` is passed.
- **Both instances are reachable only via VPC PrivateLink.** Public Bolt is
  off (or unreliable) for both source and target.
- **Domain identity is preserved via your own properties** (e.g.
  `User.user_id`). Neo4j-internal `elementId` values are NOT preserved on the
  target — see [Identity preservation](#identity-preservation--element-ids-are-not-preserved).

If your situation breaks any of these, prefer:

- `neo4j-admin database upload` — point-in-time consistent and preserves
  every internal identifier, but requires the target's public Bolt during
  upload (incompatible with PL-only).
- A backup/restore via `neo4j-admin database dump` + `upload` — same
  identifier-preservation properties, same public-Bolt requirement.
- A maintenance-window cutover with this script + a CDC replay step for
  any deltas during the run window. (CDC out of scope for this script.)

---

## EC2 mode IAM

Two principals need permissions: the **invoking machine** (where you run
`--mode=ec2`) and the **migration EC2's instance profile**.

### Invoking machine

Needs these IAM actions on the resources the script creates:

- `ec2:RunInstances`
- `ec2:TerminateInstances`
- `ec2:DescribeInstances`
- `ssm:PutParameter` and `ssm:DeleteParameter` on
  `arn:aws:ssm:<region>:<account>:parameter/aura-migration/*`
- `ssm:SendCommand` (on the launched instance + the AWS-RunShellScript
  document)
- `ssm:GetCommandInvocation`

### Migration EC2 instance profile

The instance profile attached to the migration EC2 needs:

- AWS-managed `AmazonSSMManagedInstanceCore` (so the SSM agent can register
  and execute commands).
- Inline `ssm:GetParameter` (and `GetParameters` if you need batch reads)
  on `arn:aws:ssm:<region>:<account>:parameter/aura-migration/*` so the
  bootstrap script can pull the Aura passwords into env vars before
  invoking the migration.

### How passwords transit

1. The invoking script prompts via `getpass` (or honors the
   `SOURCE_AURA_PASSWORD` / `TARGET_AURA_PASSWORD` env vars, never argv).
2. They're written to SSM Parameter Store as `SecureString` under
   `/aura-migration/<run-id>/source-password` and `…/target-password`.
3. The migration EC2's bootstrap script reads them via
   `aws ssm get-parameter --with-decryption` into environment variables.
4. The migration script consumes them from the env, never from the
   command line.
5. The invoking script's `finally` block deletes both parameters and
   terminates the EC2 even if the migration fails.

Passwords are never written to disk on the EC2, never appear in
`/proc/<pid>/cmdline`, and never appear in CloudTrail's command history
(SSM redacts SecureString values from `GetCommandInvocation` output).

---

## Multi-PrivateLink deployments

The script doesn't care whether source and target share a single
PrivateLink endpoint or live behind two — it just opens Bolt connections
to two URIs and the customer VPC's networking handles the rest.

**Same Aura orch cluster (single-PL setup).** When both Aura instances
are in the same orch cluster (their Bolt URIs share the same
`production-orch-XXXX.neo4j.io` parent), one VPC endpoint covers both
with one Private DNS zone (`*.production-orch-XXXX.neo4j.io`) and one
TLS cert.

**Different Aura orch clusters (multi-PL setup).** When source and
target are in different orch clusters, Aura returns two different
endpoint service names. Customer creates two VPC endpoints in the same
VPC — one per service — and each endpoint registers its own Private DNS
zone and presents its own TLS cert. The migration command shape is
unchanged:

```bash
python3 aura_to_aura_migration.py --mode=ec2 \
  --source-uri=neo4j+s://aaaaaaaa.production-orch-XXXX.neo4j.io \
  --target-uri=neo4j+s://bbbbbbbb.production-orch-YYYY.neo4j.io \
  ...
```

The migration EC2's outbound SG (default allow all) reaches both
endpoint ENIs. Each endpoint can use its own SG or share one — inbound
rules just need to permit `tcp/80,443,7687` from the VPC CIDR.

Cost note: each Interface VPC endpoint runs ~$0.014/hr (~$10/mo idle)
plus per-GB transit. Clean both up after the migration completes.

**Important — orch FQDN is required for PL.** Aura strips
`<dbid>.databases.neo4j.io` from public DNS for PL-only instances; the
VPC endpoint's auto-provisioned Private Hosted Zone only covers
`*.production-orch-XXXX.neo4j.io`. Always use the orch FQDN for PL-only
sources/targets.

---

## How it works

### Phase 0 — Pre-flight

Verifies connectivity to both instances, counts nodes and relationships
on the source, and checks whether the target is empty. Exits if the
target has data and you haven't passed `--overwrite`.

When `--id-map-mode=memory` is on (default), Phase 0 also estimates the
expected id_map RAM (~256 B per source node) and compares against
available host memory; fails fast (exit 4) with a sizing recommendation
if insufficient.

### Phase 1 — Schema

Runs `SHOW CONSTRAINTS` and `SHOW INDEXES` on the source, then replays
those DDL statements on the target. This runs first so constraints are
enforced as data arrives. LOOKUP indexes are skipped (Neo4j auto-creates
them); all other index types — RANGE, TEXT, POINT, VECTOR, FULLTEXT —
are migrated. Constraint backing-indexes are filtered to avoid
`EquivalentSchemaRuleAlreadyExists` warnings.

### Phase 2 — Nodes

For each node label, pages through all nodes in batches using an
**elementId cursor** (`WHERE elementId(n) > $last_eid ORDER BY elementId(n)
LIMIT batch_size`). This avoids the O(n²) cost of `SKIP/LIMIT` on large
labels. Nodes are grouped by their **full label combination** (a node
with `:Person:Employee` labels is created with both labels in one
`CREATE` — no APOC required). Each batch is written to the target with
`UNWIND ... CREATE`, and the mapping of `source elementId → target
elementId` is stored in a SQLite database on disk.

The SQLite map handles multi-label nodes correctly: if a node has
labels `[:Person, :Employee]`, it appears in both the `Person` pass and
the `Employee` pass. The second time, the script sees it's already
mapped and skips it.

Phase 2 is **single-threaded** in the current implementation. Phase 2
parallelism is out of scope for this version of the tool.

### Phase 3 — Relationships

For each relationship type:

1. **Partition discovery (when `--parallel-rels > 1`).** Query
   `count(r)` for the type. If `count >= MIN_BATCH * N`, run N-1
   `SKIP/LIMIT` boundary queries to discover N range partitions. Below
   that threshold, auto-clamp to a single full-range slot.
2. **Worker spawn.** N workers in a `ThreadPoolExecutor`, each with its
   own READ-mode source session (Bolt routing distributes them across
   cluster followers), its own target session, its own `_AutoTuneState`
   instance. The id_map is shared in-memory (default) or per-thread
   SQLite (with `--id-map-mode=sqlite`).
3. **Per-batch loop.** Each worker reads its slice via
   `WHERE elementId(r) > $last_eid AND elementId(r) <= $upper_bound`,
   resolves source→target node IDs from the id_map, writes the rel batch
   via `UNWIND ... MATCH (a) MATCH (b) CREATE (a)-[r]->(b)`, advances
   its cursor, persists checkpoint state under a shared lock.

When `--parallel-rels=1`, the worker runs in the main thread (no
ThreadPoolExecutor overhead) and the heartbeat surfaces the rich
auto-tune fields (`last_batch_rate`, `ceiling`, `soft_ceiling`).

See [Parallel relationships](#parallel-relationships---parallel-rels) below for
the deep dive on worker count selection, target-side deadlock
contention, and when raising N stops paying.

### Phase 4 — Validation

Counts nodes per label and relationships per type on both instances,
and checks that all non-LOOKUP indexes from the source are present on
the target. Prints a pass/fail table for each. Exits with code `2` if
any counts or indexes mismatch.

---

## Source consistency — assumes a quiescent source

**The source database must be effectively read-only for the duration of
the migration.** The script does not snapshot the source, does not hold
a long-running read transaction, and does not lock the source. Each
batch is its own auto-commit read transaction, so every batch sees
whatever source state exists at the moment it runs.

This is intentional — a multi-hour read transaction would block source
writes anyway, would be killed by Aura session timeouts, and would make
`--resume` impossible. The tradeoff is that the script assumes the
customer's application is paused (or routed elsewhere) during the
migration window.

### What goes wrong if the source is being written to

The script paginates with an `elementId` cursor (`WHERE elementId(n) >
$last_eid ORDER BY elementId(n)`). Whether a concurrent write is captured
or missed depends on where the cursor is when the write commits:

| Source change during migration | Result |
|---|---|
| New node, written before cursor reaches its label/elementId | ✓ copied to target |
| New node, written after cursor passed its label | ✗ missed; absent from target |
| Existing node modified before cursor reaches it | ✓ target reflects modified properties |
| Existing node modified after cursor copied it | ✗ target reflects stale (pre-modification) properties |
| Existing node deleted before cursor reaches it | ✓ correctly absent from target |
| Existing node deleted after cursor copied it | ✗ target retains deleted node — diverges from source |
| New relationship between two new nodes (both created after their label cursors passed) | ✗ start/end nodes missing from id_map; script logs `WARN: N relationships skipped (node not in ID map)` |
| Schema change on source after Phase 1 | ✗ never replayed; target schema diverges |

### How you'll know

Phase 4 (Validation) catches this at the end of the run:

```
── Phase 4: Validation ──────────────────────────────────────────
  Label                                    Source       Target   OK?
  ----------------------------------- ------------ ------------ -----
  <Label>                                <count>      <count-N>     ✗   ← N missed
  ...
  Result: FAILED ✗
```

You'll also see `WARN: N relationships skipped (node not in ID map)`
lines in Phase 3 if relationships were written between nodes that
hadn't been migrated yet. So a divergent migration won't go unnoticed
— but it does mean the target isn't usable as a clean cutover until
the divergence is resolved.

### Mitigations for live source migrations

If pausing source writes isn't acceptable, options include:

- **Maintenance window cutover.** Stop application writes, run the
  migration, switch app over to the new instance, accept the downtime.
- **Aura-native dump/restore** if your network model allows public
  connectivity — Aura's dump is point-in-time consistent.
- **Migration + CDC replay.** Run this script with the source live,
  then replay any deltas captured by CDC tooling between Phase 0 start
  and target cutover. (Out of scope for this script.)
- **Application-level dual-write with cutover.** App writes to both
  instances during a transition window; cut over reads when target is
  verified consistent. (Out of scope for this script.)

---

## Identity preservation — element IDs are NOT preserved

Unlike `neo4j-admin database upload` or a backup/restore workflow
(which copy raw store files and preserve every internal identifier),
this script **creates new nodes and relationships on the target** with
the source data's properties, labels, and types. The target's Neo4j
assigns its own `elementId` values to each new node and relationship.

| | Backup / restore | This script |
|---|---|---|
| `elementId(n)` of a node on source vs target | Identical | Different |
| `elementId(r)` of a relationship | Identical | Different |
| Internal numeric `id(n)` (deprecated) | Identical | Different |
| Domain properties (`User.user_id`, `User.name`, etc.) | Preserved | Preserved |
| Labels and relationship types | Preserved | Preserved |
| Indexes and constraints | Preserved | Replayed (see Phase 1) |

**What breaks if downstream systems reference Neo4j-internal IDs:**

- External application code that stores `elementId` (or the deprecated
  numeric `id()`) as a foreign key in another system — those references
  won't resolve on the target.
- Cypher queries that hard-code an `elementId` literal — won't find
  their node on the new instance.
- Audit logs that include `elementId` — still readable, but no longer
  point to the same physical node.

**What works fine:**

- Anything that references nodes/rels by domain properties
  (`MATCH (u:User {user_id: 12345})`) — the property-based identity
  moves through the migration intact.

The script's internal id_map (`migration_ids.db`, plus an in-memory
copy during Phase 3) preserves `source_elementId → target_elementId`
mappings *during the run* so the relationship phase can resolve
endpoints. It's not intended as a long-term lookup table and is safe
to delete once validation passes.

If preserving Neo4j-internal IDs matters for your use case (e.g., the
application encodes `elementId` in URLs or external references), this
script is the wrong tool — use `neo4j-admin database dump` +
`neo4j-admin database upload` instead, accepting the requirement that
public traffic is enabled on the target during the upload.

---

## Resume / crash recovery

After every batch the script atomically persists a **checkpoint JSON
file** tracking which labels and relationship types are complete, and
the highest source `elementId` written so far per worker (per
rel-type). If the script is interrupted, re-run with `--resume` to
continue from exactly where it left off.

**Atomicity.** `save_checkpoint` writes to a temp file then `os.replace`s
it under a module-level reentrant lock. The reentrant lock matters
because Python signal handlers run on the main thread synchronously —
if `SIGINT` arrives while the main thread is already inside
`save_checkpoint`, a non-reentrant lock would deadlock the process. The
write itself is atomic under POSIX `rename` semantics, so a process
kill mid-write either leaves the prior checkpoint intact or the new
one — never a torn JSON.

**Where the checkpoint and id_map live.** Both files default to the
**current working directory** (`migration_checkpoint.json` and
`migration_ids.db`). This means a `--resume` only works if you re-launch
from the *same directory* that ran the original migration. If you need
stable absolute paths (e.g. resuming from a different shell, a CI
runner, or a different user's session), pass
`--checkpoint-file=/abs/path.json` and `--id-map-db=/abs/path.db` on
both runs.

**Per-worker resume.** When `--parallel-rels > 1`, each worker's
progress is persisted in
`cp["rel_type_workers"][<rel_type>][<worker_id>]` with `last_eid` and
`upper_bound`. Resume rebuilds the worker partition from the saved
state — workers don't redo each other's work, and no slice goes missing.

**Checkpoint v2 → v3 shim.** The auto-tune branch (v2) used a flat
`rel_type_last_eid` cursor. The parallel-rels branch (v3) uses
per-worker state. `load_checkpoint` detects v2 mid-flight checkpoints
and converts each to a single-worker v3 entry — so customers in the
middle of a v2 migration when they upgrade can `--resume` cleanly.

**Transient errors.** Dropped Bolt connections, temporary Aura
unavailability, deadlocks, and OOM-shaped TransientErrors are retried
automatically up to 7 times with exponential backoff (2s, 5s, 15s, 30s,
60s, 120s — total ~3.5 min window). Most short network blips are
handled transparently without needing a manual `--resume`.

**Auto-tune state.** Lives in memory only — NOT included in the
checkpoint. After `--resume`, the loop bootstraps fresh and re-discovers
ceilings on the first few batches per label/rel-type. The cost is one
OOM + halve per type that previously hit OOM; on a multi-million-row
label this is seconds, not minutes.

### Caveat: duplicate relationships on resume after a hard crash

If the process dies in the small window **between** a relationship
batch's write commit and the corresponding checkpoint save, `--resume`
will re-run that batch and create duplicate relationships on the target
(Neo4j allows multiple parallel rels of the same type between the same
nodes). The window is sub-second, but it exists. If validation reports
a target relationship count higher than the source, the safest fix is
to drop the target database and re-run from scratch. This does not
affect the node phase (the id_map dedups multi-label visits and
post-crash retries by source elementId).

---

## All flags

### Connection (env var fallbacks shown)

| Flag | Env var | Description |
|---|---|---|
| `--source-uri` | `SOURCE_AURA_URI` | Source Bolt URI |
| `--source-user` | `SOURCE_AURA_USER` | Default `neo4j` |
| `--source-password` | `SOURCE_AURA_PASSWORD` | Source password |
| `--target-uri` | `TARGET_AURA_URI` | Target Bolt URI |
| `--target-user` | `TARGET_AURA_USER` | Default `neo4j` |
| `--target-password` | `TARGET_AURA_PASSWORD` | Target password |

### Behavior

| Flag | Description |
|---|---|
| `--mode` | `local` (default) or `ec2` |
| `--dry-run` | Connect and count source only; no writes |
| `--resume` | Continue from last checkpoint |
| `--overwrite` | Proceed into a non-empty target |
| `--skip-schema` | Skip constraint/index migration |
| `--skip-validation` | Skip post-migration count comparison |
| `--batch-size N` | With auto-tune ON (default), the **initial bootstrap value** when explicitly set; the loop adapts from there. With auto-tune OFF, a **fixed batch size**. (default: 10,000) |
| `--auto-tune-batch-size` / `--no-auto-tune-batch-size` | Adapt batch size at runtime based on per-batch wall-time, OOMs, and throughput plateau. Default: ON. Disable for benchmarks where reproducibility matters more than throughput. See [Adaptive batch sizing](#adaptive-batch-sizing). |
| `--parallel-rels N` | Phase 3 workers per rel type (default `2`). `1` = sequential. Auto-clamps to 1 for rel types smaller than `MIN_BATCH * N`. See [Parallel relationships](#parallel-relationships---parallel-rels). |
| `--id-map-mode` | `memory` (default) or `sqlite`. See [ID map storage modes](#id-map-storage-modes---id-map-mode). |
| `--profile-batches` | Emit per-stage timing breakdown each heartbeat. Off by default. See [Profile flag interpretation](#profile-flag-interpretation---profile-batches). |

### Output files

| Flag | Description |
|---|---|
| `--checkpoint-file` | Default `./migration_checkpoint.json` |
| `--id-map-db` | Default `./migration_ids.db` |

### EC2 mode

| Flag | Env var | Description |
|---|---|---|
| `--subnet-id` | `AWS_SUBNET_ID` | Subnet for the migration EC2 |
| `--security-group-id` | `AWS_SECURITY_GROUP_ID` | Must allow outbound 7687 |
| `--instance-profile` | `AWS_INSTANCE_PROFILE` | IAM instance profile ARN |
| `--instance-type` | — | Default `t3.large` (8 GB; covers ≤5M-node graphs in `--id-map-mode=memory`) |
| `--aws-region` | `AWS_DEFAULT_REGION` | AWS region |

---

## Adaptive batch sizing

When `--auto-tune-batch-size` is on (the default), the script picks a
per-label / per-rel-type batch size at runtime instead of using a single
global `--batch-size`. Same code, no flag changes — runs on small and
large Aura tiers and discovers the right ceiling for each from observed
signals.

### What it does

Two-level design:

1. **Bootstrap pick (initial batch only)** — based on the label /
   rel-type's total row count from Phase 0:

   | Total rows | Initial batch |
   |---|---|
   | < 5,000 | total (one shot) |
   | < 100,000 | 5,000 |
   | < 1,000,000 | 10,000 |
   | ≥ 1,000,000 | 20,000 |

   This is a starting hint only — from batch 2 onward, the adaptive
   loop is the source of truth. An explicit `--batch-size N` overrides
   the bootstrap.

2. **Adaptive loop** — three runtime signals drive it:

   - **Wall-time slow/fast.** A batch is "slow" if it took > 5 s,
     "fast" if < 1 s. After 3 consecutive slow batches → halve. After
     5 consecutive fast batches → double.
   - **`MemoryPoolOutOfMemoryError`** — sticky hard ceiling. The
     OOMing size halves and locks: doubling can never bring the loop
     back to a size that's known to OOM. Sub-divides the failed batch
     and retries at the new size; consecutive OOMs at retry are not
     coalesced — each is a real signal.
   - **Throughput plateau.** A double must improve rows/sec by ≥ 10%
     to count as progress. If it doesn't, doubling stops for the rest
     of the label/rel-type — the bottleneck has shifted from batch
     overhead to something else (network, target write throughput,
     driver serialization).

   The loop also tracks a **soft ceiling** (the highest size that has
   produced fast batches) so a transient slow window can halve, then
   climb back up to known-good without going through the
   doubling-growth path.

### Per-worker auto-tune state under `--parallel-rels`

When N workers run for a rel type, each worker has its own
`_AutoTuneState`. Workers don't share tuning: each one's transactions
hit independent transaction-memory budgets on the leader, and OOM
signals are per-transaction. If worker 0 OOMs at 40K and lowers its
ceiling to 20K, worker 1 keeps climbing independently — possibly
hitting the same OOM size before locking its own ceiling. This is
correct (each worker converges to its own ceiling) but means the
target may see up to N OOM events on a tier-too-small workload before
`InstanceTooSmall` fires.

### What you'll see

Heartbeat lines (every 20 s) gain auto-tune fields. For
`--parallel-rels=1`:

```
[<RelType>] 8,450,000/10,018,281 (84.4%) rate 5,890/s elapsed 1,434s
            last_batch_rate=12,300/s batch=20,000 ceiling=∞ soft_ceiling=20,000
```

For `--parallel-rels > 1`, a compact per-worker view replaces the rich
fields:

```
[<RelType>] 1,520,000/10,018,281 (15.2%) rate 4,050/s elapsed 375s
            workers=[w0:20,000 w1:40,000]
```

| Field | Meaning |
|---|---|
| `rate` | Cumulative rows/sec since the rel-type started |
| `last_batch_rate` (n=1 only) | Rows/sec of the most recent completed batch |
| `batch` (n=1 only) | Current batch size the loop has settled on |
| `ceiling` (n=1 only) | Hard upper bound (`∞` if never tripped) |
| `soft_ceiling` (n=1 only) | Highest size that has produced fast batches |
| `workers=[…]` (n>1 only) | Per-worker current batch sizes |

State-transition events (halve, double, OOM, plateau-lock) emit their
own stderr WARN lines so transitions are easy to grep for in long-run
logs.

### Failure modes

If the target instance can't sustain even `MIN_BATCH = 1,000` rows for
some label, the script raises `InstanceTooSmall`, prints an
operator-friendly message, preserves the checkpoint, and exits with
code **3** (distinct from generic exit 1, so EC2-mode wrappers can
recognize "this is a tier mismatch, not a transient"). Re-run with
`--resume` after upgrading the target Aura tier.

### When to disable

Pass `--no-auto-tune-batch-size` to disable. The static path is
byte-identical to pre-auto-tune behavior. Use it for:

- **Benchmark reproducibility** — fixed batches are required to compare
  runs.
- **Diagnosing whether auto-tune is implicated** in a regression.
- **Tier-cap regression tests** — pinning a known-too-large batch to
  confirm the OOM repro on a tier.

---

## Parallel relationships (`--parallel-rels`)

Phase 3 reads relationships from the source one at a time per worker
and writes them to the target one at a time per worker. Without
parallelism, both sides of that pipeline serialize through a single
session — and on the source side, Bolt routing pins all those reads to
one cluster member. Aura is always a multi-node cluster (typically 4
read-eligible members), so a single-worker script uses ~25% of the
source cluster's CPU at best.

`--parallel-rels N` partitions each rel type by `elementId` range and
spawns N workers. Each worker:

- Opens its **own source session** with `default_access_mode="READ"` so
  Bolt routing distributes it to a follower. With N=2 on a 4-member
  cluster, both readers should land on different followers (best-effort
  — driver routing decides).
- Opens its **own target session** for writes (default WRITE access —
  goes to the leader, but commits are independent transactions running
  concurrently in the leader's commit pipeline).
- Maintains its **own `_AutoTuneState`** (each worker independently
  detects OOM / fast / slow signals).
- Reads the id_map from the **shared in-memory dict** (default) or its
  **own SQLite connection** (with `--id-map-mode=sqlite`).

### Partition discovery

For each rel type at start:

1. Query `count(r)` for the type.
2. If `count < MIN_BATCH * N`: auto-clamp to a single full-range slot.
3. Else: discover N-1 boundary `elementId` values via SKIP/LIMIT:

   ```cypher
   MATCH ()-[r:`<TYPE>`]->()
   WITH r ORDER BY elementId(r)
   SKIP $offset LIMIT 1
   RETURN elementId(r) AS eid
   ```

   Each worker gets a slice `(lower_eid, upper_eid_inclusive)` such
   that workers' ranges are non-overlapping and gap-free.

The SKIP/LIMIT discovery is O(count) once per rel type — for a 10M-rel
type that's ~30–60 sec of upfront cost, amortized over the rel type's
total migration time.

### Target-side deadlock contention and the canonical-ordering mitigation

When N workers concurrently CREATE rels that touch the **same node
endpoints**, Forseti (Neo4j's locking subsystem) can detect deadlocks
on the EXCLUSIVE node-ID locks the rel writes need. Symptom in the
log:

```
WARN: transient error (attempt 1/7), retrying in 2s — TransientError:
{code: Neo.TransientError.Transaction.DeadlockDetected}
```

These are **transient** (the retry budget handles them automatically)
but each costs ~2–5 seconds of backoff time, so high frequency
translates directly to wall-time loss.

**Canonical lock ordering (built in).** Each worker's per-batch
`UNWIND ... CREATE` Cypher pre-sorts the batch by
`(min(start_eid, end_eid), max(start_eid, end_eid))`. This forces
every transaction across every parallel worker to acquire EXCLUSIVE
node locks in the same global order, eliminating the **AB/BA cycle**
class of deadlocks (the most common pattern in unsorted parallel rel
writes). Empirically, in our internal scaling tests this reduced
deadlock density at N=4 by roughly an order of magnitude.

**What canonical ordering does not eliminate.** Each worker's UNWIND
is one transaction holding all its batch's locks until commit. Across
two workers' transactions, lock cycles can still form when batches
touch overlapping nodes mid-flight. Those residual deadlocks still
fire occasionally; the retry budget absorbs them with bounded
wall-time cost. The fully-deadlock-free architectural alternative is
server-side `CALL { … } IN N CONCURRENT TRANSACTIONS OF M ROWS`
(commits per inner chunk, releases locks between), which would
replace client-side parallel-rels but would also lose the
source-read-spread-across-followers lever. Tracked as a possible
future direction; not in this release.

**Practical implication.** With canonical ordering in place,
`--parallel-rels=4` is viable for src_read-bound workloads where the
gain from a 4th reader meaningfully exceeds the residual deadlock
cost. Default of `2` is chosen as a safe lift that consistently helps
across all the bottleneck shapes we've measured; raise to `4` after
confirming via `--profile-batches` that source read time per batch
exceeds target write time per batch by a clear margin.

### When the lever helps and when it doesn't

| Bottleneck shape (per `--profile-batches`) | Effect of raising `--parallel-rels` |
|---|---|
| `src_read` dominates Phase 3 | Strong gain — readers spread across cluster followers, source-side throughput multiplies near-linearly until the cluster member count |
| `tgt_write` dominates Phase 3 | Modest gain — concurrent commits multiply leader throughput, but deadlock contention rises with N |
| Roughly balanced | Modest gain — both levers help slightly; sensitive to id_map mode (use `memory`) |
| `id_map_get` dominant under `--id-map-mode=sqlite` | Switch to `--id-map-mode=memory` first; SQLite contention is the issue |
| Phase 2 dominates total wall time | `--parallel-rels` does not affect Phase 2 (single-threaded today) |

The honest characterization: `--parallel-rels` is a Phase 3 lever
specifically for source-read-bound workloads. The default of `2` is
chosen to provide consistent benefit on the most common case while
keeping deadlock retry frequency low. **Use `--profile-batches` to
characterize your workload before raising N past the default.**

---

## ID map storage modes (`--id-map-mode`)

The id_map (`{source_eid → target_eid}`) is the bridge between Phase 2
(creates target nodes) and Phase 3 (resolves rel endpoints by source
node). Two storage backends are supported:

### `memory` (default)

At Phase 3 start, the script does a full `SELECT source_eid, target_eid
FROM id_map` against SQLite and builds an in-memory Python dict.
Workers then do `dict.get(source_eid)` per lookup, which is
GIL-protected (no contention) and ~100 ns per lookup.

Pros:
- ~100× faster per-lookup than SQLite (~5–20 µs).
- Eliminates lock contention under `--parallel-rels > 1` (we measured
  ~2.6× faster `id_map_get` per batch under N=2 vs `sqlite` mode).
- No fallback path needed — Phase 3 only reads the id_map, never
  writes.

Cons:
- Requires enough host RAM for the full map. ~256 B per source node;
  see the sizing table in [README.md](README.md#sizing--picking-an-ec2-instance-type).

Resume safety: SQLite remains the durable store for Phase 2 writes.
The in-memory dict is rebuilt from SQLite at every Phase 3 start
(including after `--resume`). No state lives only in RAM.

### `sqlite`

Each Phase 3 worker opens its own per-thread SQLite connection
(`check_same_thread=True` is satisfied because the connection is
created and used entirely within the worker thread). Each lookup
batch issues a `SELECT ... WHERE source_eid IN (?, ?, ...)`.

Pros:
- Constant memory footprint — works on tiny EC2 instances regardless
  of source size.
- Simpler to reason about under `--resume` (no rebuild step).

Cons:
- Per-lookup overhead is ~100× higher than dict.
- Concurrent readers contend on SQLite's shared-page-cache lock; under
  `--parallel-rels > 1` the per-batch `id_map_get` time grows ~2.6×
  vs single-worker.

### Picking a mode

- **Default to `memory`.** Profile data shows it's faster on every
  shape we've tested, and the sizing table covers the common case.
- **Fall back to `sqlite`** only when host RAM is tight (e.g.
  > 100M-node graphs on a t3.2xlarge) and you've accepted the
  per-lookup latency hit.
- **The preflight memory check** will tell you which mode is feasible
  on your current host before Phase 1 starts.

---

## Profile flag interpretation (`--profile-batches`)

The flag emits one extra heartbeat line per rel-type/label, summing
per-stage `time.perf_counter` deltas across the heartbeat interval.
Format:

```
[<Label>] stages src_read=<sec> id_map_get=<sec> group=<sec>
          tgt_write=<sec> id_map_put=<sec> ckpt_save=<sec> (n=<batches>)
```

Stages timed:

| Stage | What's timed |
|---|---|
| `src_read` | The source `MATCH ... LIMIT batch` query (Bolt round-trip + Aura source-side execution) |
| `id_map_get` | Resolving source elementIds → target elementIds (SQLite SELECT or in-memory dict.get depending on `--id-map-mode`) |
| `group` | Pure-Python: dedup + group-by-label-tuple + batch_data construction |
| `tgt_write` | Sum of target write Cypher executions (UNWIND + CREATE) for this batch |
| `id_map_put` | Phase 2 only — SQLite INSERT OR REPLACE of the id_map |
| `ckpt_save` | Atomic checkpoint write (tempfile + os.replace under lock) |

Counters reset each emission so the line covers *the last interval*,
not since-start. Drift is visible mid-run.

### Reading the breakdown

Add up the stage times for one batch (i.e. divide each cumulative
total by `n` to get per-batch per-worker numbers under
`--parallel-rels > 1`, since the cumulative is across n batches × N
workers). Whichever stage dominates is your bottleneck; consult the
table in [Parallel relationships](#parallel-relationships---parallel-rels)
above to pick the right lever.

### Profile flag overhead

~6 `time.perf_counter()` calls per batch — well under 1 ms total per
batch on any modern host. Off by default to keep production runs quiet,
not because the cost is significant. Turn it on once to characterize a
new workload, off for production.

---

## Sample output

```
══════════════════════════════════════════════════════════════
  Aura → Aura Migration
  Source : neo4j+s://xxxx.production-orch-XXXX.neo4j.io
  Target : neo4j+s://yyyy.production-orch-XXXX.neo4j.io
  Mode   : LIVE
  Batch  : auto-tune ON (bootstrap from per-label counts)
══════════════════════════════════════════════════════════════

── Connecting ───────────────────────────────────────────────────
  ✓ Connected to source: neo4j+s://xxxx.production-orch-XXXX.neo4j.io
  ✓ Connected to target: neo4j+s://yyyy.production-orch-XXXX.neo4j.io

── Phase 0: Pre-flight ──────────────────────────────────────────
  Source: <N> nodes, <M> relationships
  Target: 0 nodes currently
  id_map estimate : <N> nodes × ~256 B = ~<X.XX> GB
  host available  : ~<Y.YY> GB

── Phase 1: Schema ──────────────────────────────────────────────
  Constraints: <C>
  Indexes:     <I>
  Schema done.

── Phase 2: Nodes ───────────────────────────────────────────────
  Labels: <Label1>, <Label2>, <Label3>, ...

  [<Label1>] <a>/<b> (<pct>%) rate <r>/s elapsed <e>s ETA <eta>s last_batch_rate=<lbr>/s batch=<bs> ceiling=<c> soft_ceiling=<sc>
  ...
  [<Label1>] <count> new nodes (label total on source: <count>)
  [<Label2>] <count> new nodes ...

  Node migration complete: <total> nodes written

── Phase 3: Relationships ───────────────────────────────────────
  Rel types: <Type1>, <Type2>, ...
  Parallel workers per rel type: up to 2 (auto-clamped on small types)
  ID map mode: memory

  Loading id_map into memory (mode=memory) ...
  ✓ <total> entries in <t>s (~<MB> MB)

  [<Type1>] partitioned into 2 workers (rel_total=<count>)
  [<Type1>] <a>/<b> (<pct>%) rate <r>/s elapsed <e>s ETA <eta>s workers=[w0:<bs> w1:<bs>]
  ...
  [<Type1>] <count> relationships written (source total: <count>)
  ...

  Relationship migration complete: <total> relationships written

── Phase 4: Validation ──────────────────────────────────────────

  Label                                    Source       Target   OK?
  ----------------------------------- ------------ ------------ -----
  <Label1>                                <count>      <count>     ✓
  <Label2>                                <count>      <count>     ✓
  ...

  Rel Type                                 Source       Target   OK?
  ----------------------------------- ------------ ------------ -----
  <Type1>                                 <count>      <count>     ✓
  ...

  Index                                Type           OK?
  ----------------------------------- ------------ -----
  <constraint>                        RANGE            ✓
  ...

  Result: PASSED ✓

══════════════════════════════════════════════════════════════
  Done in <X.X> min
  Checkpoint : migration_checkpoint.json
  ID map     : migration_ids.db
══════════════════════════════════════════════════════════════
```

---

## Artifacts

After a successful run the working directory contains
`migration_checkpoint.json` and `migration_ids.db`. Both are safe to
delete once Phase 4 validation passes; both are excluded from git via
`.gitignore`.

If you ran with `--profile-batches`, the heartbeat output also serves
as a captured-on-disk record of per-stage timing for that workload —
useful for sizing decisions on similar future migrations.
