# Aura-to-Aura Data Copy with Private Link (AWS)

**Copy data between two PrivateLink-only Neo4j Aura instances over Bolt, entirely inside your AWS VPC.**

> **Status:** Community tool, Apache 2.0, AWS-only. Not officially supported by Neo4j — validate in non-prod first. `--mode=ec2` uses boto3 + AWS SSM + AWS PrivateLink (no GCP / Azure / cross-cloud equivalents). `--mode=local` runs from any host that already has Bolt reachability to both Auras, regardless of cloud.

---

## When to use this

This script exists for cases the built-in tools don't cover. Try those first:

| Tool | Use when | Don't use when |
|---|---|---|
| **Aura clone** (console "Clone to") | Same Aura project (tenant) and same region; clone feature supports your source/target pair | Cross-tenant (different orgs/projects); cross-region; copying into a pre-existing target you've already configured; bytes must demonstrably stay inside your VPC |
| **Aura Data Importer** | Source dump ≤ 4 GB | Dump > 4 GB |
| **[`neo4j-admin database upload`](https://support.neo4j.com/s/article/10932963739539-Using-neo4j-admin-database-upload-in-Neo4j-5-x-to-load-a-database-dump-to-Neo4j-Aura)** | Target Aura's public Bolt is reachable; you need `elementId` preservation | Target is PL-only (public Bolt off) |
| **This script** | Both Auras are PL-only **and** one of: dump > 4 GB, cross-tenant, pre-existing target, or compliance requires bytes stay in your VPC | Source can't be quiescent; `elementId` preservation required; one of the above tools fits |

Detailed criteria + alternatives for live-source migrations: [README_DETAILED.md § Use-when, and not-when](README_DETAILED.md#use-when-and-not-when).

---

## Quick start

```bash
export SOURCE_AURA_URI=neo4j+s://xxxx.production-orch-XXXX.neo4j.io
export SOURCE_AURA_PASSWORD=...
export TARGET_AURA_URI=neo4j+s://yyyy.production-orch-XXXX.neo4j.io
export TARGET_AURA_PASSWORD=...
```

**Local mode** — run from a host already inside the VPC:

```bash
python3 aura_to_aura_migration.py --mode=local --dry-run     # connectivity check
python3 aura_to_aura_migration.py --mode=local               # full migration
python3 aura_to_aura_migration.py --mode=local --resume      # continue from checkpoint
```

**EC2 mode** — script provisions an ephemeral EC2 in the VPC, runs the migration there, terminates:

```bash
python3 aura_to_aura_migration.py --mode=ec2 \
  --source-uri=$SOURCE_AURA_URI \
  --target-uri=$TARGET_AURA_URI \
  --subnet-id=subnet-0abc1234 \
  --security-group-id=sg-0abc1234 \
  --instance-profile=arn:aws:iam::123456789012:instance-profile/aura-migration
```

EC2-mode passwords are stored short-lived in SSM Parameter Store as `SecureString` and passed via env vars (never argv).

For PL-only Aura, the URI must use the orch-cluster FQDN (`<dbid>.production-orch-XXXX.neo4j.io`), not `<dbid>.databases.neo4j.io` — the latter doesn't resolve from inside a VPC. See [README_DETAILED.md § Multi-PrivateLink deployments](README_DETAILED.md#multi-privatelink-deployments).

Run `--help` for all flags.

---

## Requirements

- Python 3.9+ (`pip install -r requirements.txt`)
- Bolt (7687) reachability to both Auras (local mode), or AWS permissions to launch/terminate EC2 + use SSM (EC2 mode). IAM detail and EC2 sizing table in [README_DETAILED.md](README_DETAILED.md).

---

## Reference

[README_DETAILED.md](README_DETAILED.md) — flags, multi-PL, source-consistency, identity preservation, resume semantics, adaptive batching, parallel relationships, profile-flag interpretation, EC2 sizing, sample output.

After a successful run: `migration_checkpoint.json` and `migration_ids.db` (gitignored, safe to delete once Phase 4 validation passes).
