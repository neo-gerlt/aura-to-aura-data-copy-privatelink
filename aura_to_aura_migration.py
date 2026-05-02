#!/usr/bin/env python3
"""Aura-to-Aura database migration using the Neo4j Python driver over Bolt.

Works with Private Link (public traffic disabled) because Bolt (port 7687)
is proxied by the VPC Private Link endpoint. Run from an EC2 in the VPC that
has Private Link endpoints for both source and target Aura instances.

Phases:
  0 - Pre-flight    Connectivity check + source node/rel counts
  1 - Schema        Constraints and indexes
  2 - Nodes         Batch copy by label; source→target ID map persisted in SQLite
  3 - Relationships Batch copy by type using ID map
  4 - Validation    Compare per-label and per-type counts source vs target

Usage:
  export SOURCE_AURA_URI=neo4j+s://xxxx.databases.neo4j.io
  export SOURCE_AURA_PASSWORD=secret
  export TARGET_AURA_URI=neo4j+s://yyyy.databases.neo4j.io
  export TARGET_AURA_PASSWORD=secret
  python3 aura_to_aura_migration.py
  python3 aura_to_aura_migration.py --dry-run
  python3 aura_to_aura_migration.py --resume
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

from neo4j import GraphDatabase, Driver
from neo4j.exceptions import (
    AuthError,
    DatabaseUnavailable,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)

# ── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_BATCH_SIZE = 5000
DEFAULT_CHECKPOINT_FILE = "migration_checkpoint.json"
DEFAULT_ID_MAP_DB = "migration_ids.db"
DEFAULT_USER = "neo4j"

CHECKPOINT_VERSION = 2  # bumped when the checkpoint schema changes incompatibly

_RETRY_DELAYS = (2, 5, 15, 30, 60, 120)  # seconds between attempts on transient failures
# Total retry window: ~3.5 min. Sized for Aura connection hiccups that can last
# tens of seconds during sustained load (observed in real test runs).

# Matches the leading "CREATE [<TYPE>] INDEX" form produced by SHOW INDEXES YIELD createStatement
# in Neo4j 5.x. Used to inject "IF NOT EXISTS" idempotently across RANGE/TEXT/POINT/VECTOR/FULLTEXT.
_INDEX_CREATE_RE = re.compile(
    r"^(CREATE\s+(?:FULLTEXT|RANGE|TEXT|POINT|VECTOR|LOOKUP)?\s*INDEX)\s+(?!IF\s+NOT\s+EXISTS\b)",
    re.IGNORECASE,
)

# In Neo4j 5.x, "IF NOT EXISTS" must come AFTER the constraint name, not after "CREATE CONSTRAINT".
# So we insert it before the FOR keyword instead. Matches both named ("CREATE CONSTRAINT `n` FOR …")
# and unnamed ("CREATE CONSTRAINT FOR …") forms; the createStatement always contains FOR.
_CONSTRAINT_FOR_RE = re.compile(r"\sFOR\s", re.IGNORECASE)

# ── Checkpoint ─────────────────────────────────────────────────────────────────

def load_checkpoint(path: str) -> dict:
    if Path(path).exists():
        with open(path) as f:
            cp = json.load(f)
        if cp.get("version") != CHECKPOINT_VERSION:
            print(
                f"  Incompatible checkpoint format (got version "
                f"{cp.get('version', 'unversioned')!r}, expected {CHECKPOINT_VERSION}).\n"
                f"  Delete {path} and the ID map and re-run without --resume to start fresh.",
                file=sys.stderr,
            )
            sys.exit(1)
        return cp
    return {
        "version": CHECKPOINT_VERSION,
        "schema_done": False,
        "nodes_done": False,
        "rels_done": False,
        "labels_complete": [],
        "label_last_eid": {},
        "rel_types_complete": [],
        "rel_type_last_eid": {},
        "started_at": datetime.now(timezone.utc).isoformat(),
    }


def save_checkpoint(path: str, cp: dict) -> None:
    cp["updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(path, "w") as f:
        json.dump(cp, f, indent=2)


# ── ID Map (SQLite) ────────────────────────────────────────────────────────────

def open_id_map(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS id_map "
        "(source_eid TEXT PRIMARY KEY, target_eid TEXT NOT NULL)"
    )
    conn.commit()
    return conn


def id_map_put(conn: sqlite3.Connection, rows: List[Tuple[str, str]]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO id_map (source_eid, target_eid) VALUES (?, ?)", rows
    )
    conn.commit()


def id_map_get(conn: sqlite3.Connection, source_eids: List[str]) -> Dict[str, str]:
    """Return {source_eid: target_eid} for all source_eids present in the map."""
    if not source_eids:
        return {}
    placeholders = ",".join("?" * len(source_eids))
    cur = conn.execute(
        f"SELECT source_eid, target_eid FROM id_map WHERE source_eid IN ({placeholders})",
        source_eids,
    )
    return {row[0]: row[1] for row in cur.fetchall()}


# ── Driver helpers ─────────────────────────────────────────────────────────────

def open_driver(uri: str, user: str, password: str, label: str) -> Driver:
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        print(f"  ✓ Connected to {label}: {uri}")
        return driver
    except AuthError:
        print(f"  ✗ Auth failed for {label}: {uri}", file=sys.stderr)
        raise
    except ServiceUnavailable:
        print(f"  ✗ Cannot reach {label}: {uri}", file=sys.stderr)
        raise


def run_query(driver: Driver, cypher: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
    last_exc: Optional[Exception] = None
    for attempt, delay in enumerate((*_RETRY_DELAYS, None)):
        try:
            with driver.session() as session:
                result = session.run(cypher, params or {})
                return [record.data() for record in result]
        except (TransientError, ServiceUnavailable, SessionExpired, DatabaseUnavailable) as exc:
            # SessionExpired covers mid-query bolt connection death (idle TCP timeouts on Aura,
            # NLB resets, etc.); each retry opens a fresh session so this is safe to retry.
            last_exc = exc
            if delay is None:
                break
            print(
                f"  WARN: transient error (attempt {attempt + 1}/{len(_RETRY_DELAYS) + 1}), "
                f"retrying in {delay}s — {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


def count_query(driver: Driver, cypher: str, params: Optional[Dict] = None) -> int:
    return run_query(driver, cypher, params)[0]["cnt"]


# ── Phase 0: Pre-flight ────────────────────────────────────────────────────────

def preflight(src: Driver, tgt: Driver, overwrite: bool) -> Dict[str, int]:
    print("\n── Phase 0: Pre-flight ──────────────────────────────────────────")

    src_nodes = count_query(src, "MATCH (n) RETURN count(n) AS cnt")
    src_rels  = count_query(src, "MATCH ()-[r]->() RETURN count(r) AS cnt")
    tgt_nodes = count_query(tgt, "MATCH (n) RETURN count(n) AS cnt")

    print(f"  Source: {src_nodes:,} nodes, {src_rels:,} relationships")
    print(f"  Target: {tgt_nodes:,} nodes currently")

    if tgt_nodes > 0 and not overwrite:
        print(
            "\n  ERROR: Target database is not empty. "
            "Pass --overwrite to proceed anyway.",
            file=sys.stderr,
        )
        sys.exit(1)

    return {"src_nodes": src_nodes, "src_rels": src_rels}


# ── Phase 1: Schema ────────────────────────────────────────────────────────────

def migrate_schema(src: Driver, tgt: Driver, dry_run: bool) -> None:
    print("\n── Phase 1: Schema ──────────────────────────────────────────────")

    constraints = run_query(src, "SHOW CONSTRAINTS YIELD createStatement")
    print(f"  Constraints: {len(constraints)}")
    for row in constraints:
        stmt = row["createStatement"]
        # In Neo4j 5.x, "IF NOT EXISTS" must come AFTER the constraint name (before FOR),
        # not right after "CREATE CONSTRAINT" — a naive prefix replace produces invalid Cypher.
        if not re.search(r"\bIF\s+NOT\s+EXISTS\b", stmt, re.IGNORECASE):
            stmt = _CONSTRAINT_FOR_RE.sub(" IF NOT EXISTS FOR ", stmt, count=1)
        if dry_run:
            print(f"    DRY: {stmt[:100]}")
        else:
            try:
                run_query(tgt, stmt)
            except Exception as exc:
                print(f"    WARN skipping constraint: {exc} — {stmt[:80]}")

    # LOOKUP indexes are auto-created by Neo4j; all others are migrated
    indexes = run_query(
        src,
        "SHOW INDEXES YIELD type, createStatement "
        "WHERE type IN ['RANGE', 'TEXT', 'POINT', 'VECTOR', 'FULLTEXT']",
    )
    print(f"  Indexes:     {len(indexes)}")
    for row in indexes:
        stmt = row["createStatement"]
        # Insert IF NOT EXISTS after the leading CREATE [<TYPE>] INDEX, regardless of type
        # (RANGE/TEXT/POINT/VECTOR/FULLTEXT/LOOKUP). A naive substring replace misses the
        # typed forms because they don't contain the literal "CREATE INDEX".
        stmt = _INDEX_CREATE_RE.sub(r"\1 IF NOT EXISTS ", stmt, count=1)
        if dry_run:
            print(f"    DRY: {stmt[:100]}")
        else:
            try:
                run_query(tgt, stmt)
            except Exception as exc:
                print(f"    WARN skipping index: {exc} — {stmt[:80]}")

    print("  Schema done.")


# ── Phase 2: Nodes ─────────────────────────────────────────────────────────────

def _label_clause(sorted_labels: tuple) -> str:
    """Return a Cypher label string like :LabelA:`Label B`:LabelC."""
    return "".join(f":`{lbl}`" for lbl in sorted_labels)


def migrate_nodes(
    src: Driver,
    tgt: Driver,
    id_map: sqlite3.Connection,
    cp: dict,
    cp_path: str,
    batch_size: int,
    total_nodes: int,
) -> int:
    print("\n── Phase 2: Nodes ───────────────────────────────────────────────")

    labels = [r["label"] for r in run_query(src, "CALL db.labels() YIELD label")]
    print(f"  Labels: {', '.join(labels)}\n")

    migrated = 0
    pbar = tqdm(total=total_nodes, unit="nodes") if HAS_TQDM else None

    for label in labels:
        if label in cp["labels_complete"]:
            already = count_query(src, f"MATCH (n:`{label}`) RETURN count(n) AS cnt")
            if pbar:
                pbar.update(already)
            print(f"  [{label}] already complete — skipping")
            continue

        # Cursor on elementId — avoids the O(n²) cost of SKIP/LIMIT on large labels.
        # An empty string sorts before any non-empty elementId, giving a clean start cursor.
        last_eid = cp["label_last_eid"].get(label, "")
        label_total = count_query(src, f"MATCH (n:`{label}`) RETURN count(n) AS cnt")
        label_written = 0

        if last_eid:
            done_so_far = count_query(
                src,
                f"MATCH (n:`{label}`) WHERE elementId(n) <= $last_eid RETURN count(n) AS cnt",
                {"last_eid": last_eid},
            )
            print(f"  [{label}] resuming after {done_so_far:,}/{label_total:,}")
            if pbar:
                pbar.update(done_so_far)

        while True:
            records = run_query(
                src,
                f"MATCH (n:`{label}`) WHERE elementId(n) > $last_eid "
                "RETURN elementId(n) AS eid, labels(n) AS labels, properties(n) AS props "
                f"ORDER BY elementId(n) LIMIT {batch_size}",
                {"last_eid": last_eid},
            )
            if not records:
                break

            # Bulk-check which source eids are already in the map (multi-label dedup)
            known = id_map_get(id_map, [r["eid"] for r in records])
            new_records = [r for r in records if r["eid"] not in known]

            if new_records:
                # Group by full sorted label tuple → one CREATE per unique label combo
                # avoids any dependency on APOC
                groups: Dict[tuple, list] = {}
                for r in new_records:
                    key = tuple(sorted(r["labels"]))
                    groups.setdefault(key, []).append(r)

                for label_combo, group in groups.items():
                    label_str = _label_clause(label_combo)
                    cypher = (
                        f"UNWIND $batch AS row "
                        f"CREATE (n{label_str}) "
                        "SET n = row.props "
                        "RETURN row.source_eid AS source_eid, elementId(n) AS target_eid"
                    )
                    batch_data = [{"source_eid": r["eid"], "props": r["props"]} for r in group]
                    results = run_query(tgt, cypher, {"batch": batch_data})
                    id_map_put(id_map, [(r["source_eid"], r["target_eid"]) for r in results])

            count = len(records)
            last_eid = records[-1]["eid"]
            label_written += len(new_records)
            migrated += len(new_records)

            if pbar:
                pbar.update(len(new_records))

            cp["label_last_eid"][label] = last_eid
            save_checkpoint(cp_path, cp)

            if count < batch_size:
                break

        cp["labels_complete"].append(label)
        cp["label_last_eid"].pop(label, None)
        save_checkpoint(cp_path, cp)
        print(f"  [{label}] {label_written:,} new nodes (label total on source: {label_total:,})")

    if pbar:
        pbar.close()

    cp["nodes_done"] = True
    save_checkpoint(cp_path, cp)
    print(f"\n  Node migration complete: {migrated:,} nodes written")
    return migrated


# ── Phase 3: Relationships ─────────────────────────────────────────────────────

def migrate_relationships(
    src: Driver,
    tgt: Driver,
    id_map: sqlite3.Connection,
    cp: dict,
    cp_path: str,
    batch_size: int,
    total_rels: int,
) -> int:
    print("\n── Phase 3: Relationships ───────────────────────────────────────")

    rel_types = [
        r["relationshipType"]
        for r in run_query(src, "CALL db.relationshipTypes() YIELD relationshipType")
    ]
    print(f"  Rel types: {', '.join(rel_types)}\n")

    migrated = 0
    pbar = tqdm(total=total_rels, unit="rels") if HAS_TQDM else None

    for rel_type in rel_types:
        if rel_type in cp["rel_types_complete"]:
            already = count_query(src, f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS cnt")
            if pbar:
                pbar.update(already)
            print(f"  [{rel_type}] already complete — skipping")
            continue

        last_eid = cp["rel_type_last_eid"].get(rel_type, "")
        rel_total = count_query(src, f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS cnt")
        rel_written = 0

        if last_eid:
            done_so_far = count_query(
                src,
                f"MATCH ()-[r:`{rel_type}`]->() WHERE elementId(r) <= $last_eid "
                "RETURN count(r) AS cnt",
                {"last_eid": last_eid},
            )
            print(f"  [{rel_type}] resuming after {done_so_far:,}/{rel_total:,}")
            if pbar:
                pbar.update(done_so_far)

        while True:
            records = run_query(
                src,
                f"MATCH (a)-[r:`{rel_type}`]->(b) WHERE elementId(r) > $last_eid "
                "RETURN elementId(r) AS r_eid, elementId(a) AS start_eid, "
                "       elementId(b) AS end_eid, properties(r) AS props "
                f"ORDER BY elementId(r) LIMIT {batch_size}",
                {"last_eid": last_eid},
            )
            if not records:
                break

            # Resolve source elementIds → target elementIds in one SQLite query
            all_source_eids = list(
                {r["start_eid"] for r in records} | {r["end_eid"] for r in records}
            )
            eid_map = id_map_get(id_map, all_source_eids)

            batch_data = []
            missing = 0
            for r in records:
                t_start = eid_map.get(r["start_eid"])
                t_end   = eid_map.get(r["end_eid"])
                if t_start and t_end:
                    batch_data.append({
                        "start_eid": t_start,
                        "end_eid":   t_end,
                        "props":     r["props"],
                    })
                else:
                    missing += 1

            if missing:
                print(f"    WARN: {missing} relationships skipped (node not in ID map)")

            if batch_data:
                cypher = (
                    "UNWIND $batch AS row "
                    "MATCH (a) WHERE elementId(a) = row.start_eid "
                    "MATCH (b) WHERE elementId(b) = row.end_eid "
                    f"CREATE (a)-[r:`{rel_type}`]->(b) "
                    "SET r = row.props "
                    "RETURN count(r) AS created"
                )
                run_query(tgt, cypher, {"batch": batch_data})

            count = len(records)
            last_eid = records[-1]["r_eid"]
            rel_written += len(batch_data)
            migrated += len(batch_data)

            if pbar:
                pbar.update(count)

            cp["rel_type_last_eid"][rel_type] = last_eid
            save_checkpoint(cp_path, cp)

            if count < batch_size:
                break

        cp["rel_types_complete"].append(rel_type)
        cp["rel_type_last_eid"].pop(rel_type, None)
        save_checkpoint(cp_path, cp)
        print(f"  [{rel_type}] {rel_written:,} relationships written (source total: {rel_total:,})")

    if pbar:
        pbar.close()

    cp["rels_done"] = True
    save_checkpoint(cp_path, cp)
    print(f"\n  Relationship migration complete: {migrated:,} relationships written")
    return migrated


# ── Phase 4: Validation ────────────────────────────────────────────────────────

def validate(src: Driver, tgt: Driver) -> bool:
    print("\n── Phase 4: Validation ──────────────────────────────────────────")
    passed = True

    src_labels = [r["label"] for r in run_query(src, "CALL db.labels() YIELD label")]
    tgt_labels_set = {r["label"] for r in run_query(tgt, "CALL db.labels() YIELD label")}

    print(f"\n  {'Label':<35} {'Source':>12} {'Target':>12} {'OK?':>5}")
    print(f"  {'-'*35} {'-'*12} {'-'*12} {'-'*5}")
    for label in sorted(src_labels):
        src_cnt = count_query(src, f"MATCH (n:`{label}`) RETURN count(n) AS cnt")
        tgt_cnt = count_query(tgt, f"MATCH (n:`{label}`) RETURN count(n) AS cnt") if label in tgt_labels_set else 0
        ok = src_cnt == tgt_cnt
        if not ok:
            passed = False
        print(f"  {label:<35} {src_cnt:>12,} {tgt_cnt:>12,} {'✓' if ok else '✗':>5}")

    src_rel_types = [r["relationshipType"] for r in run_query(src, "CALL db.relationshipTypes() YIELD relationshipType")]
    tgt_rel_types_set = {r["relationshipType"] for r in run_query(tgt, "CALL db.relationshipTypes() YIELD relationshipType")}

    print(f"\n  {'Rel Type':<35} {'Source':>12} {'Target':>12} {'OK?':>5}")
    print(f"  {'-'*35} {'-'*12} {'-'*12} {'-'*5}")
    for rel_type in sorted(src_rel_types):
        src_cnt = count_query(src, f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS cnt")
        tgt_cnt = count_query(tgt, f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS cnt") if rel_type in tgt_rel_types_set else 0
        ok = src_cnt == tgt_cnt
        if not ok:
            passed = False
        print(f"  {rel_type:<35} {src_cnt:>12,} {tgt_cnt:>12,} {'✓' if ok else '✗':>5}")

    src_indexes = {
        r["name"]: r["type"]
        for r in run_query(src, "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP'")
    }
    tgt_indexes = {
        r["name"]
        for r in run_query(tgt, "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP'")
    }

    print(f"\n  {'Index':<35} {'Type':<12} {'OK?':>5}")
    print(f"  {'-'*35} {'-'*12} {'-'*5}")
    for name in sorted(src_indexes):
        ok = name in tgt_indexes
        if not ok:
            passed = False
        print(f"  {name:<35} {src_indexes[name]:<12} {'✓' if ok else '✗ MISSING':>5}")

    print(f"\n  Result: {'PASSED ✓' if passed else 'FAILED ✗'}")
    return passed


# ── EC2 mode ──────────────────────────────────────────────────────────────────

def _wait_for_ssm(ssm_client: Any, instance_id: str, timeout: int = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = ssm_client.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
        )
        if resp["InstanceInformationList"]:
            return
        time.sleep(10)
    raise TimeoutError(f"SSM agent not ready on {instance_id} after {timeout}s")


def _stream_ssm_output(ssm_client: Any, instance_id: str, command_id: str) -> int:
    time.sleep(5)  # give SSM time to register the invocation before first poll
    printed = 0
    while True:
        try:
            result = ssm_client.get_command_invocation(
                CommandId=command_id, InstanceId=instance_id
            )
        except ssm_client.exceptions.InvocationDoesNotExist:
            time.sleep(5)
            continue

        status = result["Status"]
        stdout = result.get("StandardOutputContent", "")
        stderr = result.get("StandardErrorContent", "")

        if len(stdout) > printed:
            print(stdout[printed:], end="", flush=True)
            printed = len(stdout)

        if status in ("Success", "Failed", "Cancelled", "TimedOut"):
            if stderr:
                print(stderr, file=sys.stderr)
            return result["ResponseCode"]

        time.sleep(10)


def _build_passthrough_flags(args: argparse.Namespace) -> str:
    flags: List[str] = []
    if args.dry_run:         flags.append("--dry-run")
    if args.overwrite:       flags.append("--overwrite")
    if args.resume:          flags.append("--resume")
    if args.skip_schema:     flags.append("--skip-schema")
    if args.skip_validation: flags.append("--skip-validation")
    if args.source_user != DEFAULT_USER:
        flags.append(f"--source-user {args.source_user}")
    if args.target_user != DEFAULT_USER:
        flags.append(f"--target-user {args.target_user}")
    if args.batch_size != DEFAULT_BATCH_SIZE:
        flags.append(f"--batch-size {args.batch_size}")
    return " ".join(flags)


def run_ec2_mode(args: argparse.Namespace) -> None:
    try:
        import boto3
        import getpass
        import uuid
    except ImportError:
        print("EC2 mode requires boto3: pip install boto3", file=sys.stderr)
        sys.exit(1)

    print("═" * 62)
    print("  Aura → Aura Migration  [EC2 mode]")
    print(f"  Source  : {args.source_uri}")
    print(f"  Target  : {args.target_uri}")
    print(f"  Subnet  : {args.subnet_id}")
    print(f"  Type    : {args.instance_type}")
    print("═" * 62)

    source_password = getpass.getpass("\nSource Aura password: ")
    target_password = getpass.getpass("Target Aura password: ")

    run_id = uuid.uuid4().hex[:8]
    ssm_prefix = f"/aura-migration/{run_id}"
    src_pw_path = f"{ssm_prefix}/source-password"
    tgt_pw_path = f"{ssm_prefix}/target-password"

    session = boto3.Session(region_name=args.aws_region or None)
    region = session.region_name
    ec2_client = session.client("ec2")
    ssm_client = session.client("ssm")

    print(f"\nStoring credentials in SSM ({ssm_prefix})...")
    ssm_client.put_parameter(Name=src_pw_path, Value=source_password, Type="SecureString", Overwrite=True)
    ssm_client.put_parameter(Name=tgt_pw_path, Value=target_password, Type="SecureString", Overwrite=True)

    instance_id = None
    exit_code = 1
    try:
        ami_id = ssm_client.get_parameter(
            Name="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
        )["Parameter"]["Value"]

        print(f"Launching {args.instance_type} (AMI: {ami_id})...")
        instance_id = ec2_client.run_instances(
            ImageId=ami_id,
            InstanceType=args.instance_type,
            MinCount=1,
            MaxCount=1,
            SubnetId=args.subnet_id,
            SecurityGroupIds=[args.security_group_id],
            IamInstanceProfile={"Arn": args.instance_profile},
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name",                  "Value": f"aura-migration-{run_id}"},
                    {"Key": "aura-migration-run-id", "Value": run_id},
                ],
            }],
            InstanceInitiatedShutdownBehavior="terminate",
        )["Instances"][0]["InstanceId"]
        print(f"  Instance: {instance_id}")

        print("  Waiting for instance running...")
        ec2_client.get_waiter("instance_running").wait(InstanceIds=[instance_id])
        print("  Waiting for SSM agent...")
        _wait_for_ssm(ssm_client, instance_id)
        print("  Ready.\n")

        passthrough = _build_passthrough_flags(args)

        # Embed the local script via base64 chunks instead of fetching from GitHub.
        # This guarantees the EC2 runs exactly the local code (no "main vs working tree"
        # foot-gun, no supply-chain risk from an unpinned raw URL).
        script_bytes = Path(__file__).resolve().read_bytes()
        script_b64 = base64.b64encode(script_bytes).decode()
        chunk_size = 4096
        chunks = [script_b64[i : i + chunk_size] for i in range(0, len(script_b64), chunk_size)]

        command_lines: List[str] = [
            "#!/bin/bash",
            "set -euo pipefail",
            "pip3 install -q neo4j tqdm",
            "rm -f /tmp/migrate.b64 /tmp/migrate.py",
        ]
        for chunk in chunks:
            command_lines.append(f'printf "%s" "{chunk}" >> /tmp/migrate.b64')
        command_lines.extend([
            "base64 -d /tmp/migrate.b64 > /tmp/migrate.py",
            "rm -f /tmp/migrate.b64",
            # Aura passwords go through env vars, not argv, so they don't leak via /proc.
            f'export SOURCE_AURA_PASSWORD=$(aws ssm get-parameter --region {region} --name "{src_pw_path}" --with-decryption --query Parameter.Value --output text)',
            f'export TARGET_AURA_PASSWORD=$(aws ssm get-parameter --region {region} --name "{tgt_pw_path}" --with-decryption --query Parameter.Value --output text)',
            f'python3 /tmp/migrate.py --mode=local --source-uri="{args.source_uri}" --target-uri="{args.target_uri}" {passthrough}',
        ])

        cmd_resp = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": command_lines},
            TimeoutSeconds=7200,
        )
        command_id = cmd_resp["Command"]["CommandId"]
        print(f"SSM command: {command_id}\n{'─' * 62}")

        exit_code = _stream_ssm_output(ssm_client, instance_id, command_id)

    finally:
        print("\n── Cleanup ──────────────────────────────────────────────────────")
        for path in (src_pw_path, tgt_pw_path):
            try:
                ssm_client.delete_parameter(Name=path)
            except Exception as exc:
                print(f"  WARN: could not delete {path}: {exc}", file=sys.stderr)
        print(f"  SSM parameters deleted ({ssm_prefix})")
        if instance_id:
            try:
                ec2_client.terminate_instances(InstanceIds=[instance_id])
                print(f"  Terminated {instance_id}")
            except Exception as exc:
                print(f"  WARN: could not terminate {instance_id}: {exc}", file=sys.stderr)

    sys.exit(exit_code)


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migrate a Neo4j Aura database to another Aura instance via Bolt.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    conn = p.add_argument_group("Connection (env var fallbacks shown)")
    conn.add_argument("--source-uri",      default=os.environ.get("SOURCE_AURA_URI"),
                      metavar="URI",       help="Source bolt URI (SOURCE_AURA_URI)")
    conn.add_argument("--source-user",     default=os.environ.get("SOURCE_AURA_USER", DEFAULT_USER))
    conn.add_argument("--source-password", default=os.environ.get("SOURCE_AURA_PASSWORD"),
                      metavar="PW",        help="Source password (SOURCE_AURA_PASSWORD)")
    conn.add_argument("--target-uri",      default=os.environ.get("TARGET_AURA_URI"),
                      metavar="URI",       help="Target bolt URI (TARGET_AURA_URI)")
    conn.add_argument("--target-user",     default=os.environ.get("TARGET_AURA_USER", DEFAULT_USER))
    conn.add_argument("--target-password", default=os.environ.get("TARGET_AURA_PASSWORD"),
                      metavar="PW",        help="Target password (TARGET_AURA_PASSWORD)")

    beh = p.add_argument_group("Behavior")
    beh.add_argument("--mode",             choices=["local", "ec2"], default="local",
                     help="Run locally or provision an EC2 in the customer VPC (default: local)")
    beh.add_argument("--batch-size",       type=int, default=DEFAULT_BATCH_SIZE,
                     help=f"Nodes/rels per transaction (default: {DEFAULT_BATCH_SIZE})")
    beh.add_argument("--overwrite",        action="store_true",
                     help="Proceed even if target is not empty")
    beh.add_argument("--resume",           action="store_true",
                     help="Resume from existing checkpoint file")
    beh.add_argument("--dry-run",          action="store_true",
                     help="Connect and count source only; no writes")
    beh.add_argument("--skip-schema",      action="store_true",
                     help="Skip constraint/index migration")
    beh.add_argument("--skip-validation",  action="store_true",
                     help="Skip post-migration count comparison")

    out = p.add_argument_group("Output files")
    out.add_argument("--checkpoint-file",  default=DEFAULT_CHECKPOINT_FILE,
                     help=f"Checkpoint JSON path (default: {DEFAULT_CHECKPOINT_FILE})")
    out.add_argument("--id-map-db",        default=DEFAULT_ID_MAP_DB,
                     help=f"SQLite ID map path (default: {DEFAULT_ID_MAP_DB})")

    ec2 = p.add_argument_group("EC2 mode (required when --mode=ec2)")
    ec2.add_argument("--subnet-id",         default=os.environ.get("AWS_SUBNET_ID"),
                     help="Subnet ID for the migration EC2 (AWS_SUBNET_ID)")
    ec2.add_argument("--security-group-id", default=os.environ.get("AWS_SECURITY_GROUP_ID"),
                     help="Security group ID — must allow outbound 7687 (AWS_SECURITY_GROUP_ID)")
    ec2.add_argument("--instance-profile",  default=os.environ.get("AWS_INSTANCE_PROFILE"),
                     help="IAM instance profile ARN — needs ssm:GetParameter (AWS_INSTANCE_PROFILE)")
    ec2.add_argument("--instance-type",     default="t3.medium",
                     help="EC2 instance type (default: t3.medium)")
    ec2.add_argument("--aws-region",        default=os.environ.get("AWS_DEFAULT_REGION"),
                     help="AWS region (default: AWS_DEFAULT_REGION or boto3 default)")

    return p.parse_args()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    # Validate required arguments
    missing = []
    if not args.source_uri: missing.append("--source-uri / SOURCE_AURA_URI")
    if not args.target_uri: missing.append("--target-uri / TARGET_AURA_URI")
    if args.mode == "local":
        if not args.source_password: missing.append("--source-password / SOURCE_AURA_PASSWORD")
        if not args.target_password: missing.append("--target-password / TARGET_AURA_PASSWORD")
    if missing:
        print("Missing required arguments:\n  " + "\n  ".join(missing), file=sys.stderr)
        sys.exit(1)

    if args.mode == "ec2":
        ec2_missing = []
        if not args.subnet_id:         ec2_missing.append("--subnet-id / AWS_SUBNET_ID")
        if not args.security_group_id: ec2_missing.append("--security-group-id / AWS_SECURITY_GROUP_ID")
        if not args.instance_profile:  ec2_missing.append("--instance-profile / AWS_INSTANCE_PROFILE")
        if ec2_missing:
            print("EC2 mode requires:\n  " + "\n  ".join(ec2_missing), file=sys.stderr)
            sys.exit(1)
        run_ec2_mode(args)
        return

    # Checkpoint guard
    if Path(args.checkpoint_file).exists() and not args.resume and not args.dry_run:
        print(
            f"Checkpoint file already exists: {args.checkpoint_file}\n"
            "  --resume   to continue from last saved position\n"
            f"  rm {args.checkpoint_file} {args.id_map_db}   to start fresh"
        )
        sys.exit(1)

    cp = load_checkpoint(args.checkpoint_file)

    print("═" * 62)
    print("  Aura → Aura Migration")
    print(f"  Source : {args.source_uri}")
    print(f"  Target : {args.target_uri}")
    print(f"  Mode   : {'DRY RUN (no writes)' if args.dry_run else 'LIVE'}")
    print(f"  Batch  : {args.batch_size:,} rows/tx")
    print("═" * 62)

    start_time = time.time()

    src = tgt = id_map = None
    try:
        # Open connections
        print("\n── Connecting ───────────────────────────────────────────────────")
        src = open_driver(args.source_uri, args.source_user, args.source_password, "source")
        tgt = open_driver(args.target_uri, args.target_user, args.target_password, "target")
        id_map = open_id_map(args.id_map_db)

        # Save checkpoint on Ctrl-C so --resume can pick up cleanly
        def _handle_sigint(sig, frame):
            print("\n\n  Interrupted — checkpoint saved. Re-run with --resume to continue.")
            save_checkpoint(args.checkpoint_file, cp)
            if src: src.close()
            if tgt: tgt.close()
            if id_map: id_map.close()
            sys.exit(130)

        signal.signal(signal.SIGINT, _handle_sigint)
        # ── Phase 0 ───────────────────────────────────────────────────────────
        # --resume implies the target is expected to have prior-run data.
        counts = preflight(src, tgt, args.overwrite or args.dry_run or args.resume)

        if args.dry_run:
            print("\n  Dry run complete — no data written.")
            return

        # ── Phase 1 ───────────────────────────────────────────────────────────
        if args.skip_schema:
            print("\n── Phase 1: Schema — SKIPPED (--skip-schema)")
        elif cp.get("schema_done"):
            print("\n── Phase 1: Schema — SKIPPED (already complete)")
        else:
            migrate_schema(src, tgt, args.dry_run)
            cp["schema_done"] = True
            save_checkpoint(args.checkpoint_file, cp)

        # ── Phase 2 ───────────────────────────────────────────────────────────
        if cp.get("nodes_done"):
            print("\n── Phase 2: Nodes — SKIPPED (already complete)")
        else:
            migrate_nodes(
                src, tgt, id_map, cp, args.checkpoint_file,
                args.batch_size, counts["src_nodes"],
            )

        # ── Phase 3 ───────────────────────────────────────────────────────────
        if cp.get("rels_done"):
            print("\n── Phase 3: Relationships — SKIPPED (already complete)")
        else:
            migrate_relationships(
                src, tgt, id_map, cp, args.checkpoint_file,
                args.batch_size, counts["src_rels"],
            )

        # ── Phase 4 ───────────────────────────────────────────────────────────
        passed = True
        if args.skip_validation:
            print("\n── Phase 4: Validation — SKIPPED (--skip-validation)")
        else:
            passed = validate(src, tgt)

        elapsed = time.time() - start_time
        print(f"\n{'═' * 62}")
        print(f"  Done in {elapsed / 60:.1f} min")
        print(f"  Checkpoint : {args.checkpoint_file}")
        print(f"  ID map     : {args.id_map_db}")
        print("═" * 62)

        sys.exit(0 if passed else 2)

    finally:
        if src: src.close()
        if tgt: tgt.close()
        if id_map: id_map.close()


if __name__ == "__main__":
    main()
