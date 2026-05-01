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
import json
import os
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
from neo4j.exceptions import AuthError, ServiceUnavailable

# ── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_BATCH_SIZE = 5000
DEFAULT_CHECKPOINT_FILE = "migration_checkpoint.json"
DEFAULT_ID_MAP_DB = "migration_ids.db"
DEFAULT_USER = "neo4j"

# ── Checkpoint ─────────────────────────────────────────────────────────────────

def load_checkpoint(path: str) -> dict:
    if Path(path).exists():
        with open(path) as f:
            return json.load(f)
    return {
        "schema_done": False,
        "nodes_done": False,
        "rels_done": False,
        "labels_complete": [],
        "label_offsets": {},
        "rel_types_complete": [],
        "rel_type_offsets": {},
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
    with driver.session() as session:
        result = session.run(cypher, params or {})
        return [record.data() for record in result]


def count_query(driver: Driver, cypher: str) -> int:
    return run_query(driver, cypher)[0]["cnt"]


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
        if "IF NOT EXISTS" not in stmt:
            stmt = stmt.replace("CREATE CONSTRAINT", "CREATE CONSTRAINT IF NOT EXISTS", 1)
        if dry_run:
            print(f"    DRY: {stmt[:100]}")
        else:
            try:
                run_query(tgt, stmt)
            except Exception as exc:
                print(f"    WARN skipping constraint: {exc} — {stmt[:80]}")

    # Skip LOOKUP (auto-created) and FULLTEXT (complex to recreate safely)
    indexes = run_query(
        src,
        "SHOW INDEXES YIELD type, createStatement "
        "WHERE type IN ['RANGE', 'TEXT', 'POINT']",
    )
    print(f"  Indexes:     {len(indexes)}")
    for row in indexes:
        stmt = row["createStatement"]
        if "IF NOT EXISTS" not in stmt:
            stmt = stmt.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1)
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

        skip = cp["label_offsets"].get(label, 0)
        label_total = count_query(src, f"MATCH (n:`{label}`) RETURN count(n) AS cnt")
        label_written = 0

        if skip > 0:
            print(f"  [{label}] resuming at offset {skip:,}/{label_total:,}")
            if pbar:
                pbar.update(skip)

        while True:
            # SKIP/LIMIT is safe here because source is read-only during migration
            records = run_query(
                src,
                f"MATCH (n:`{label}`) "
                "RETURN elementId(n) AS eid, labels(n) AS labels, properties(n) AS props "
                f"SKIP {skip} LIMIT {batch_size}",
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
            skip += count
            label_written += len(new_records)
            migrated += len(new_records)

            if pbar:
                pbar.update(len(new_records))

            cp["label_offsets"][label] = skip
            save_checkpoint(cp_path, cp)

            if count < batch_size:
                break

        cp["labels_complete"].append(label)
        cp["label_offsets"].pop(label, None)
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

        skip = cp["rel_type_offsets"].get(rel_type, 0)
        rel_total = count_query(src, f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS cnt")
        rel_written = 0

        if skip > 0:
            print(f"  [{rel_type}] resuming at offset {skip:,}/{rel_total:,}")
            if pbar:
                pbar.update(skip)

        while True:
            records = run_query(
                src,
                f"MATCH (a)-[r:`{rel_type}`]->(b) "
                "RETURN elementId(a) AS start_eid, elementId(b) AS end_eid, "
                "       properties(r) AS props "
                f"SKIP {skip} LIMIT {batch_size}",
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
            skip += count
            rel_written += len(batch_data)
            migrated += len(batch_data)

            if pbar:
                pbar.update(count)

            cp["rel_type_offsets"][rel_type] = skip
            save_checkpoint(cp_path, cp)

            if count < batch_size:
                break

        cp["rel_types_complete"].append(rel_type)
        cp["rel_type_offsets"].pop(rel_type, None)
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

    print(f"\n  Result: {'PASSED ✓' if passed else 'FAILED ✗'}")
    return passed


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

    return p.parse_args()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    # Validate required credentials
    missing = []
    if not args.source_uri:      missing.append("--source-uri / SOURCE_AURA_URI")
    if not args.source_password: missing.append("--source-password / SOURCE_AURA_PASSWORD")
    if not args.target_uri:      missing.append("--target-uri / TARGET_AURA_URI")
    if not args.target_password: missing.append("--target-password / TARGET_AURA_PASSWORD")
    if missing:
        print("Missing required arguments:\n  " + "\n  ".join(missing), file=sys.stderr)
        sys.exit(1)

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

    # Open connections
    print("\n── Connecting ───────────────────────────────────────────────────")
    src = open_driver(args.source_uri, args.source_user, args.source_password, "source")
    tgt = open_driver(args.target_uri, args.target_user, args.target_password, "target")
    id_map = open_id_map(args.id_map_db)

    # Save checkpoint on Ctrl-C so --resume can pick up cleanly
    def _handle_sigint(sig, frame):
        print("\n\n  Interrupted — checkpoint saved. Re-run with --resume to continue.")
        save_checkpoint(args.checkpoint_file, cp)
        src.close()
        tgt.close()
        id_map.close()
        sys.exit(130)

    signal.signal(signal.SIGINT, _handle_sigint)

    try:
        # ── Phase 0 ───────────────────────────────────────────────────────────
        counts = preflight(src, tgt, args.overwrite or args.dry_run)

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
        src.close()
        tgt.close()
        id_map.close()


if __name__ == "__main__":
    main()
