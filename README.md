# Aura-to-Aura Data Copy with Private Link

**Securely copy data between two Neo4j Aura instances entirely inside your VPC, over PrivateLink, with no traffic crossing the public internet.**

This is the alternative to `neo4j-admin database upload`, which is point-in-time consistent and preserves Neo4j-internal identifiers, but requires the target's public Bolt to be reachable during the upload — incompatible with PL-only Aura instances.

The script connects to both Aura instances simultaneously over Bolt (port 7687), reads from the source, and writes to the target in batches across four phases (schema → nodes → relationships → validation).

---

## Key features

- **Bolt-only over PrivateLink** — works when Aura's public traffic is disabled.
- **Two run modes:**
  - `--mode=local` runs from a host already inside the customer VPC.
  - `--mode=ec2` provisions a fresh EC2 in the VPC, embeds itself onto the instance, runs the migration there, streams output back in near-real time, and terminates the EC2 automatically.
- **Adaptive batch sizing** (`--auto-tune-batch-size`, default ON) — discovers per-label / per-rel-type batch ceilings from runtime signals (`MemoryPoolOutOfMemoryError`, fast/slow per-batch wall time, throughput plateau) instead of relying on operator guesswork. Same flags work across small and large Aura tiers.
- **Phase 3 parallelism** (`--parallel-rels N`, default `2`) — N workers per rel type, each with its own READ-mode source session so Bolt routing distributes reads across the source cluster's followers; concurrent target writes multiply leader commit throughput.
- **In-memory id_map** (`--id-map-mode=memory`, default) — Phase 3 reads the source→target node mapping from a Python dict instead of SQLite, eliminating lock contention under parallel workers. SQLite stays the durable store for Phase 2 writes + `--resume`.
- **Per-stage profile breakdown** (`--profile-batches`) — emits per-batch timing for `src_read`, `id_map_get`, `group`, `tgt_write`, `id_map_put`, `ckpt_save` so operators can characterize their workload's bottleneck and tune accordingly.
- **Atomic checkpoint + resume** — every batch persists state atomically (tempfile + `os.replace` under a reentrant lock); `--resume` picks up from the saved cursor with byte-equivalent semantics across restarts. Per-worker progress is preserved.
- **Phase 4 validation** — count comparison per label / rel type plus index-presence check; non-zero exit on any mismatch.

---

## Use this script when

- Both Aura instances are reachable only via PrivateLink (public Bolt disabled).
- The source can be made effectively read-only for the migration window — see [Source consistency](README_DETAILED.md#source-consistency--assumes-a-quiescent-source).
- Domain identity is preserved through your own properties (e.g. `User.user_id`). Neo4j-internal `elementId` values are NOT preserved on the target — see [Identity preservation](README_DETAILED.md#identity-preservation--element-ids-are-not-preserved).

If any of these aren't true, see [Mitigations](README_DETAILED.md#mitigations-for-live-source-migrations) for alternatives.

---

## Requirements

- Python 3.9+
- `pip install -r requirements.txt`
- Local mode: Bolt (port 7687) reachability to both Aura instances.
- EC2 mode: AWS credentials with permissions to launch/terminate EC2, write SSM parameters, and send SSM commands. The migration EC2's instance profile needs `ssm:GetParameter` on `/aura-migration/*` and the AWS-managed `AmazonSSMManagedInstanceCore` policy. Full IAM detail in [README_DETAILED.md § EC2 mode IAM](README_DETAILED.md#ec2-mode-iam).

---

## Quick start

### Local mode (already inside the VPC)

```bash
export SOURCE_AURA_URI=neo4j+s://xxxx.production-orch-XXXX.neo4j.io
export SOURCE_AURA_PASSWORD=secret
export TARGET_AURA_URI=neo4j+s://yyyy.production-orch-XXXX.neo4j.io
export TARGET_AURA_PASSWORD=secret

python3 aura_to_aura_migration.py --mode=local --dry-run     # connectivity check
python3 aura_to_aura_migration.py --mode=local               # full migration
python3 aura_to_aura_migration.py --mode=local --resume      # continue from checkpoint
```

### EC2 mode (auto-provision in the VPC)

```bash
python3 aura_to_aura_migration.py --mode=ec2 \
  --source-uri=neo4j+s://xxxx.production-orch-XXXX.neo4j.io \
  --target-uri=neo4j+s://yyyy.production-orch-XXXX.neo4j.io \
  --subnet-id=subnet-0abc1234 \
  --security-group-id=sg-0abc1234 \
  --instance-profile=arn:aws:iam::123456789012:instance-profile/aura-migration
```

Aura passwords are prompted interactively, or honored from `SOURCE_AURA_PASSWORD` / `TARGET_AURA_PASSWORD` env vars when set. Passwords stored short-lived in SSM Parameter Store as `SecureString` and pulled into the migration EC2's env vars (never argv) before the migration starts.

For PL-only Aura, the URI must use the orch-cluster FQDN (`<dbid>.production-orch-XXXX.neo4j.io`), not `<dbid>.databases.neo4j.io` — the latter form is stripped from public DNS and won't resolve from inside the VPC. See [README_DETAILED.md § Multi-PrivateLink deployments](README_DETAILED.md#multi-privatelink-deployments).

All connection parameters can also be passed as CLI flags — run `--help` for the full list.

---

## Sizing — picking an EC2 instance type

The default `--id-map-mode=memory` loads the full source→target node mapping into a Python dict at Phase 3 start (~256 B per source node), eliminating SQLite lock contention under parallel workers. Total host RAM needed = id_map + ~2 GB OS/Python overhead + ~500 MB headroom. EC2 sizing follows directly from your source's node count:

| Source node count | Recommended `--instance-type` |
|---|---|
| ≤ 1M | `t3.medium` (4 GB) |
| 1M – 5M | **`t3.large` (8 GB)** ← script default |
| 5M – 15M | `t3.xlarge` (16 GB) |
| 15M – 50M | `t3.2xlarge` (32 GB) |
| 50M – 100M | `m6i.4xlarge` (64 GB) |
| > 100M | `m6i.8xlarge` (128 GB) or larger |

The script's preflight reports the estimated id_map RAM and the host's available memory before Phase 1 starts. If memory is insufficient, it fails fast (exit `4`) with a sizing recommendation. You can override with `--id-map-mode=sqlite` to fall back to per-worker SQLite reads — works on smaller instances but slower under `--parallel-rels > 1`.

---

## Characterizing your workload (`--profile-batches`)

Different workloads bottleneck on different stages: **source read** (when one cluster member pins sequential reads), **target write** (commit pipeline), **id_map_get** (under heavy parallelism with `--id-map-mode=sqlite`), or **Bolt round-trip serialization**. The `--profile-batches` flag emits per-stage timing each heartbeat:

```
[Label] stages src_read=11.5s id_map_get=0.15s group=0.14s tgt_write=10.0s id_map_put=0.50s ckpt_save=0.001s (n=7)
```

Read this as: of `n` batches in the last heartbeat interval, how much wall time was spent in each stage (cumulative across the interval). Resets each emission so drift is visible mid-run.

| If your profile shows | Lever |
|---|---|
| `src_read` dominates Phase 3 | Raise `--parallel-rels` (lets workers land on cluster followers) |
| `tgt_write` dominates Phase 3 | Larger target Aura tier (more commit pipeline headroom); `--parallel-rels` also helps via concurrent transactions on the leader |
| `id_map_get` non-trivial under `--parallel-rels > 1` | Ensure `--id-map-mode=memory` (default) |
| `ckpt_save` non-trivial | Phase 2 fsync overhead from atomic checkpoint writes; tradeoff for torn-write safety under parallel workers |
| Phase 2 wall dominates total | Phase 2 is single-threaded today; consider a larger target Aura tier or pre-warming on the source |

The profile flag is off by default (negligible overhead, but noisy output). Turn it on for one run to learn your bottleneck, off for production runs.

---

## More detail

For the deeper reference — flag-by-flag explanations, multi-PL deployments, source-consistency caveats, identity preservation tradeoffs, resume semantics, sample output, and adaptive-batch-sizing internals — see **[README_DETAILED.md](README_DETAILED.md)**.

---

## Artifacts

After a successful run the working directory contains `migration_checkpoint.json` and `migration_ids.db`. Both are safe to delete once Phase 4 validation passes; both are gitignored.
