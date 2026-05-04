#!/usr/bin/env python3
"""MTG dataset loader for Aura — used as a second-shape data source for
parallel-rels validation in `aura_to_aura_migration.py`.

The CSVs are downloaded to local disk (curl on the EC2 in --mode=ec2),
parsed with Python's csv module (which is lenient on RFC-4180-imperfect
quoting that LOAD CSV server-side rejects), and streamed into Aura via
UNWIND-batched writes. This sidesteps two problems with naive LOAD CSV:
the upstream cards.csv has embedded double-quotes that aren't
RFC-4180-doubled (LOAD CSV fails on them), and Aura's outbound HTTP
fetch path adds latency we don't need.

Schema: Card, Set, Price, Ruling, ForeignText nodes; IN_SET, PRICED_AT,
HAS_RULING, HAS_FOREIGN rels — chosen to give a multi-rel-type
distribution distinct from any single-rel-type-dominant shape.

Modes:
  --mode=local : assumes CSVs exist in --csv-dir and runs Cypher via Bolt
  --mode=ec2   : spawn EC2 in the test VPC, curl CSVs into /tmp/, embed
                 self, run on EC2 in --mode=local with --csv-dir=/tmp

Usage:
  export TARGET_AURA_URI=neo4j+s://<dbid>.production-orch-XXXX.neo4j.io
  export TARGET_AURA_PASSWORD=secret
  python3 mtg_load.py --mode=ec2 --subnet-id=... --security-group-id=...
"""

from __future__ import annotations

import argparse
import base64
import csv
import gzip
import os
import sys
import time
import uuid as _uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ── Schema ─────────────────────────────────────────────────────────────────────

CONSTRAINTS: List[str] = [
    "CREATE CONSTRAINT card_uuid IF NOT EXISTS FOR (c:Card) REQUIRE c.uuid IS UNIQUE",
    "CREATE CONSTRAINT set_code  IF NOT EXISTS FOR (s:Set)  REQUIRE s.code IS UNIQUE",
]

# Two kinds of steps:
#   1. CSV-driven: (label, csv_filename, cypher_template). The cypher uses
#      `UNWIND $batch AS row` and is invoked once per 1000-row batch with
#      $batch bound to a list of dict rows. Python's csv module parses the
#      CSV lenient enough to handle the upstream's RFC-4180-imperfect quoting.
#   2. Pure-cypher: (label, None, cypher). Runs as-is once. Used for derived
#      rels like IN_SET that come from already-loaded data, batched
#      server-side via CALL { ... } IN TRANSACTIONS.
LOAD_STEPS: List[Tuple[str, Optional[str], str]] = [
    ("Cards", "cards.csv", """
UNWIND $batch AS row
MERGE (c:Card {uuid: row.uuid})
SET c.name          = row.name,
    c.manaCost      = row.manaCost,
    c.manaValue     = toFloat(row.manaValue),
    c.type          = row.type,
    c.types         = row.types,
    c.subtypes      = row.subtypes,
    c.supertypes    = row.supertypes,
    c.text          = row.text,
    c.flavorText    = row.flavorText,
    c.originalText  = row.originalText,
    c.rarity        = row.rarity,
    c.setCode       = row.setCode,
    c.layout        = row.layout,
    c.colors        = row.colors,
    c.colorIdentity = row.colorIdentity,
    c.power         = row.power,
    c.toughness     = row.toughness,
    c.loyalty       = row.loyalty,
    c.edhrecRank    = toFloat(row.edhrecRank)
"""),
    ("Sets", "sets.csv", """
UNWIND $batch AS row
MERGE (s:Set {code: row.code})
SET s.name         = row.name,
    s.releaseDate  = row.releaseDate,
    s.totalSetSize = toInteger(row.totalSetSize),
    s.type         = row.type,
    s.block        = row.block
"""),
    ("IN_SET rels", None, """
MATCH (c:Card)
CALL { WITH c
  MATCH (s:Set {code: c.setCode})
  MERGE (c)-[:IN_SET]->(s)
} IN TRANSACTIONS OF 1000 ROWS
"""),
    ("Prices + PRICED_AT", "cardPrices.csv", """
UNWIND $batch AS row
MATCH (c:Card {uuid: row.uuid})
CREATE (p:Price {
  date:             row.date,
  price:            toFloat(row.price),
  currency:         row.currency,
  gameAvailability: row.gameAvailability,
  priceProvider:    row.priceProvider,
  providerListing:  row.providerListing,
  cardFinish:       row.cardFinish
})
CREATE (c)-[:PRICED_AT]->(p)
"""),
    ("Rulings + HAS_RULING", "cardRulings.csv", """
UNWIND $batch AS row
MATCH (c:Card {uuid: row.uuid})
CREATE (r:Ruling {date: row.date, text: row.text})
CREATE (c)-[:HAS_RULING]->(r)
"""),
    ("ForeignText + HAS_FOREIGN", "cardForeignData.csv", """
UNWIND $batch AS row
MATCH (c:Card {uuid: row.uuid})
CREATE (f:ForeignText {
  language:     row.language,
  name:         row.name,
  faceName:     row.faceName,
  text:         row.text,
  flavorText:   row.flavorText,
  multiverseId: toFloat(row.multiverseId)
})
CREATE (c)-[:HAS_FOREIGN]->(f)
"""),
]

BATCH_SIZE = 1000

VERIFY_QUERIES: List[Tuple[str, str]] = [
    ("Card",        "MATCH (n:Card)        RETURN count(n) AS c"),
    ("Set",         "MATCH (n:Set)         RETURN count(n) AS c"),
    ("Price",       "MATCH (n:Price)       RETURN count(n) AS c"),
    ("Ruling",      "MATCH (n:Ruling)      RETURN count(n) AS c"),
    ("ForeignText", "MATCH (n:ForeignText) RETURN count(n) AS c"),
    ("IN_SET",      "MATCH ()-[r:IN_SET]->()      RETURN count(r) AS c"),
    ("PRICED_AT",   "MATCH ()-[r:PRICED_AT]->()   RETURN count(r) AS c"),
    ("HAS_RULING",  "MATCH ()-[r:HAS_RULING]->()  RETURN count(r) AS c"),
    ("HAS_FOREIGN", "MATCH ()-[r:HAS_FOREIGN]->() RETURN count(r) AS c"),
]


# ── Local mode: drive Bolt directly ────────────────────────────────────────────

def _iter_batches(csv_path: Path, size: int) -> Iterable[List[Dict[str, str]]]:
    """Yield batches of `size` row dicts from `csv_path`. Empty strings are
    coerced to None so Cypher's toFloat()/toInteger() treat them as null
    rather than barfing on '' input."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch: List[Dict[str, str]] = []
        for raw in reader:
            row = {k: (v if v != "" else None) for k, v in raw.items()}
            batch.append(row)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch


def load_local(uri: str, user: str, password: str, csv_dir: str) -> int:
    from neo4j import GraphDatabase

    print(f"\n  Connecting to {uri} ...")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    print(f"  ✓ Connected.")
    print(f"  CSV dir: {csv_dir}")

    print("\n── Constraints ─────────────────────────────────────────────────")
    for stmt in CONSTRAINTS:
        with driver.session() as session:
            session.run(stmt).consume()
        print(f"  ✓ {stmt.splitlines()[0][:80]}")

    print("\n── Loading ─────────────────────────────────────────────────────")
    overall_t0 = time.time()
    for label, csv_name, cypher in LOAD_STEPS:
        t0 = time.time()
        if csv_name is None:
            # Pure-Cypher step (e.g. IN_SET derivation from existing nodes).
            print(f"  ▸ {label} ...", flush=True)
            with driver.session() as session:
                session.run(cypher).consume()
            print(f"    done in {time.time() - t0:.1f}s", flush=True)
            continue

        csv_path = Path(csv_dir) / csv_name
        if not csv_path.exists():
            print(f"  ✗ {csv_name} not found at {csv_path}", file=sys.stderr)
            return 2
        rows_loaded = 0
        last_emit = t0
        print(f"  ▸ {label} ({csv_name}) ...", flush=True)
        for batch in _iter_batches(csv_path, BATCH_SIZE):
            with driver.session() as session:
                session.run(cypher, {"batch": batch}).consume()
            rows_loaded += len(batch)
            now = time.time()
            if now - last_emit >= 5.0:
                rate = rows_loaded / (now - t0) if now > t0 else 0
                print(f"    {rows_loaded:>10,} rows  {rate:,.0f}/s", flush=True)
                last_emit = now
        elapsed = time.time() - t0
        rate = rows_loaded / elapsed if elapsed > 0 else 0
        print(f"    {rows_loaded:>10,} rows  {rate:,.0f}/s  done in {elapsed:.1f}s",
              flush=True)

    total = time.time() - overall_t0

    print("\n── Verification ────────────────────────────────────────────────")
    for label, stmt in VERIFY_QUERIES:
        with driver.session() as session:
            cnt = session.run(stmt).single()["c"]
        print(f"  {label:<14} {cnt:>10,}")

    print(f"\n  Total load time: {total / 60:.1f} min")
    driver.close()
    return 0


# ── EC2 mode: spawn an EC2 in the test VPC and run --mode=local on it ─────────

def load_ec2(args: argparse.Namespace) -> None:
    import boto3
    import getpass

    print("═" * 62)
    print("  MTG Loader  [EC2 mode]")
    print(f"  Target  : {args.target_uri}")
    print(f"  Subnet  : {args.subnet_id}")
    print("═" * 62)

    target_password = args.target_password or getpass.getpass("\nTarget Aura password: ")

    run_id = _uuid.uuid4().hex[:8]
    ssm_prefix = f"/mtg-load/{run_id}"
    pw_path = f"{ssm_prefix}/password"

    session = boto3.Session(region_name=args.aws_region or None)
    region = session.region_name
    ec2_client = session.client("ec2")
    ssm_client = session.client("ssm")

    print(f"\nStoring credentials in SSM ({ssm_prefix}) ...")
    ssm_client.put_parameter(
        Name=pw_path, Value=target_password, Type="SecureString", Overwrite=True
    )

    instance_id = None
    exit_code = 1
    try:
        ami_id = ssm_client.get_parameter(
            Name="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
        )["Parameter"]["Value"]
        print(f"Launching {args.instance_type} (AMI: {ami_id}) ...")
        run_resp = ec2_client.run_instances(
            ImageId=ami_id,
            InstanceType=args.instance_type,
            MinCount=1, MaxCount=1,
            SubnetId=args.subnet_id,
            SecurityGroupIds=[args.security_group_id],
            IamInstanceProfile={"Arn": args.instance_profile},
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Owner", "Value": "chris.gerlt@neo4j.com"},
                    {"Key": "Task",  "Value": "tsk-20260504-mtg-load"},
                    {"Key": "Name",  "Value": f"mtg-load-{run_id}"},
                ],
            }],
        )
        instance_id = run_resp["Instances"][0]["InstanceId"]
        print(f"  Instance: {instance_id}")
        print("  Waiting for instance running ...")
        ec2_client.get_waiter("instance_running").wait(InstanceIds=[instance_id])
        print("  Waiting for SSM agent ...")
        _wait_for_ssm(ssm_client, instance_id)
        print("  Ready.\n")

        # Embed self via gzip + base64 chunks (raw base64 of any non-trivial
        # script blows past SSM SendCommand's 97 KB document limit).
        script_bytes = Path(__file__).resolve().read_bytes()
        script_b64 = base64.b64encode(gzip.compress(script_bytes)).decode()
        chunk_size = 4096
        chunks = [script_b64[i : i + chunk_size]
                  for i in range(0, len(script_b64), chunk_size)]

        csv_files = ["cards.csv", "sets.csv", "cardPrices.csv",
                     "cardRulings.csv", "cardForeignData.csv"]

        command_lines: List[str] = [
            "#!/bin/bash",
            "set -euo pipefail",
            "sudo dnf -y -q install python3-pip",
            "pip3 install -q neo4j",
            "rm -f /tmp/mtg_load.b64 /tmp/mtg_load.py",
            "mkdir -p /tmp/mtg-csv",
        ]
        # Fetch CSVs onto the EC2 — Python's csv module parses them
        # leniently in load_local; Aura's strict LOAD CSV server-side
        # rejects the upstream's RFC-4180-imperfect quoting on cards.csv.
        for f in csv_files:
            command_lines.append(
                f'curl -sSL --fail "https://mtgjson.com/api/v5/csv/{f}" '
                f'-o /tmp/mtg-csv/{f}'
            )
        for chunk in chunks:
            command_lines.append(f'printf "%s" "{chunk}" >> /tmp/mtg_load.b64')
        command_lines.extend([
            "base64 -d /tmp/mtg_load.b64 | gunzip > /tmp/mtg_load.py",
            "rm -f /tmp/mtg_load.b64",
            f'export TARGET_AURA_PASSWORD=$(aws ssm get-parameter '
            f'--region {region} --name "{pw_path}" --with-decryption '
            f'--query Parameter.Value --output text)',
            f'python3 -u /tmp/mtg_load.py --mode=local '
            f'--target-uri="{args.target_uri}" --target-user="{args.target_user}" '
            f'--csv-dir=/tmp/mtg-csv '
            f'2>&1 | tee /tmp/mtg_load.log',
        ])

        cmd_resp = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={
                "commands": command_lines,
                "executionTimeout": ["7200"],  # 2 hours
            },
            TimeoutSeconds=7200,
        )
        command_id = cmd_resp["Command"]["CommandId"]
        print(f"SSM command: {command_id}\n{'─' * 62}")
        exit_code = _stream_ssm_output(ssm_client, instance_id, command_id)
    finally:
        print("\n── Cleanup ────────────────────────────────────────────────")
        try:
            ssm_client.delete_parameter(Name=pw_path)
            print(f"  SSM parameter deleted ({ssm_prefix})")
        except Exception as exc:
            print(f"  WARN delete: {exc}", file=sys.stderr)
        if instance_id:
            try:
                ec2_client.terminate_instances(InstanceIds=[instance_id])
                print(f"  Terminated {instance_id}")
            except Exception as exc:
                print(f"  WARN terminate: {exc}", file=sys.stderr)
    sys.exit(exit_code)


# ── SSM helpers (self-contained copy from aura_to_aura_migration.py) ──────────
# Kept as a copy rather than importing so the loader doesn't pin the migration
# script's API mid-flight during in-progress migration tests.

def _wait_for_ssm(ssm_client: Any, instance_id: str, timeout: int = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = ssm_client.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
        )
        if resp.get("InstanceInformationList"):
            return
        time.sleep(3)
    raise TimeoutError(f"SSM agent not online for {instance_id}")


def _tail_remote_log(ssm_client: Any, instance_id: str, byte_offset: int) -> str:
    cmd = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [
            f"tail -c +{byte_offset} /tmp/mtg_load.log 2>/dev/null || true"
        ]},
    )
    tail_id = cmd["Command"]["CommandId"]
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            result = ssm_client.get_command_invocation(
                CommandId=tail_id, InstanceId=instance_id
            )
        except ssm_client.exceptions.InvocationDoesNotExist:
            time.sleep(0.5)
            continue
        if result["Status"] in ("Success", "Failed", "Cancelled", "TimedOut"):
            return result.get("StandardOutputContent", "") or ""
        time.sleep(0.5)
    return ""


def _stream_ssm_output(ssm_client: Any, instance_id: str, command_id: str) -> int:
    time.sleep(5)
    bytes_seen = 0
    while True:
        try:
            main = ssm_client.get_command_invocation(
                CommandId=command_id, InstanceId=instance_id
            )
            main_status = main["Status"]
        except ssm_client.exceptions.InvocationDoesNotExist:
            main_status = "InProgress"
            main = {"ResponseCode": -1, "StandardErrorContent": ""}
        new_bytes = _tail_remote_log(ssm_client, instance_id, bytes_seen + 1)
        if new_bytes:
            sys.stdout.write(new_bytes)
            sys.stdout.flush()
            bytes_seen += len(new_bytes.encode("utf-8"))
        if main_status in ("Success", "Failed", "Cancelled", "TimedOut"):
            err = main.get("StandardErrorContent", "")
            if err:
                print(err, file=sys.stderr)
            return main.get("ResponseCode", 1)
        time.sleep(3)


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load MTG dataset into an Aura instance.")
    p.add_argument("--mode", choices=["local", "ec2"], default="local",
                   help="local: run Cypher directly via Bolt. "
                        "ec2: spawn an EC2 in the test VPC and run --mode=local on it "
                        "(required when Aura is PL-only).")
    p.add_argument("--target-uri",      default=os.environ.get("TARGET_AURA_URI"),
                   metavar="URI", help="Bolt URI (TARGET_AURA_URI)")
    p.add_argument("--target-user",     default=os.environ.get("TARGET_AURA_USER", "neo4j"))
    p.add_argument("--target-password", default=os.environ.get("TARGET_AURA_PASSWORD"),
                   metavar="PW", help="(TARGET_AURA_PASSWORD)")
    p.add_argument("--subnet-id",         default=os.environ.get("AWS_SUBNET_ID"))
    p.add_argument("--security-group-id", default=os.environ.get("AWS_SECURITY_GROUP_ID"))
    p.add_argument("--instance-profile",  default=os.environ.get("AWS_INSTANCE_PROFILE"))
    p.add_argument("--instance-type",     default="t3.medium")
    p.add_argument("--aws-region",        default=os.environ.get("AWS_DEFAULT_REGION"))
    p.add_argument("--csv-dir",           default=os.environ.get("MTG_CSV_DIR", "/tmp/mtg-csv"),
                   help="Directory containing the MTG CSV files "
                        "(used by --mode=local; --mode=ec2 fetches into /tmp/mtg-csv)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.target_uri:
        print("Missing --target-uri / TARGET_AURA_URI", file=sys.stderr)
        sys.exit(1)
    if args.mode == "ec2":
        for needed, label in (
            (args.subnet_id, "--subnet-id / AWS_SUBNET_ID"),
            (args.security_group_id, "--security-group-id / AWS_SECURITY_GROUP_ID"),
            (args.instance_profile, "--instance-profile / AWS_INSTANCE_PROFILE"),
        ):
            if not needed:
                print(f"EC2 mode requires {label}", file=sys.stderr)
                sys.exit(1)
        load_ec2(args)
    else:
        if not args.target_password:
            print("Missing --target-password / TARGET_AURA_PASSWORD", file=sys.stderr)
            sys.exit(1)
        sys.exit(load_local(args.target_uri, args.target_user,
                            args.target_password, args.csv_dir))


if __name__ == "__main__":
    main()
