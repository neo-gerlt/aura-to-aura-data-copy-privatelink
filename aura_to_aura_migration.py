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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

DEFAULT_BATCH_SIZE = 10000  # tuned from a real Aura-to-Aura run; see README "Performance"
DEFAULT_CHECKPOINT_FILE = "migration_checkpoint.json"
DEFAULT_ID_MAP_DB = "migration_ids.db"
DEFAULT_USER = "neo4j"

CHECKPOINT_VERSION = 3  # bumped when the checkpoint schema changes incompatibly

DEFAULT_PARALLEL_RELS = 2  # workers per rel type in Phase 3 — 1 = sequential (pre-v3 behavior)

# Module-level shutdown signal so the SIGINT handler can ask Phase 3 workers
# to exit between batches. Workers poll between batches; in-flight batches
# finish (or fail their retries) before the worker returns.
_shutdown_event = threading.Event()

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

# Emit per-label/per-rel-type progress every N seconds so unattended runs (e.g. --mode=ec2,
# where stdout is captured by SSM and tqdm's carriage-return updates don't survive) still
# show liveness. tqdm continues to be the primary feedback when running on a TTY.
HEARTBEAT_INTERVAL_SEC = 20.0


# ── Auto-tune (self-tuning batch sizing) ───────────────────────────────────────
# See contexts/projects/aura-to-aura-data-copy-pl/tsk-20260502-aura-migration-auto-tune-batch.md
# in the CommandCenter repo for the full design.

# Server status codes that surface as OOM-shaped TransientError. The canonical
# list lives at https://neo4j.com/docs/status-codes/current/ and can shift across
# Neo4j server versions. A TransientError whose .code contains "OutOfMemory" but
# is NOT in this set emits a stderr warning so we surface drift early.
OOM_CODES = frozenset({
    "Neo.TransientError.General.MemoryPoolOutOfMemoryError",
    "Neo.TransientError.General.OutOfMemoryError",
    "Neo.TransientError.General.TransactionOutOfMemoryError",
})

# Auto-tune algorithm constants. Internal-only — promoted to CLI flags only if the
# test matrix shows we need them.
MIN_BATCH = 1_000          # floor — fail loud rather than crawl below this
SLOW_SEC = 5.0             # batches > this are "slow"
FAST_SEC = 1.0             # batches < this are "fast"
SLOW_THRESHOLD = 3         # halve after N consecutive slow batches
FAST_THRESHOLD = 5         # double after N consecutive fast batches
PLATEAU_RATIO = 1.10       # double must improve rows/sec by ≥10% to count as progress


class InstanceTooSmall(Exception):
    """Raised when an OOM forces the auto-tune ceiling below MIN_BATCH for a label.

    Caught at top-level main() for an operator-friendly message and a distinct
    exit code. Implies the target Aura instance can't sustain even MIN_BATCH-row
    batches for this label/rel-type — the user should upgrade the tier.
    """

    def __init__(self, label: str, oom_size: int, original_exc: Exception) -> None:
        self.label = label
        self.oom_size = oom_size
        self.original_exc = original_exc
        super().__init__(
            f"Auto-tune cannot proceed: {label!r} OOM'd at batch_size={oom_size:,} "
            f"and halving would drop below MIN_BATCH={MIN_BATCH:,}. "
            f"The target instance is too small for this dataset — upgrade the tier "
            f"and re-run with --resume.\n  Original error: {original_exc}"
        )


class _StageTimings:
    """Per-stage cumulative timing accumulator for `--profile-batches`.

    Stages are accumulated since the last `format_and_reset()` call, so
    each emitted line covers one heartbeat interval — making slow drift
    visible mid-run rather than smearing it across the whole label.
    Thread-safe via a lock so parallel workers can share an instance.
    """

    STAGES = ("src_read", "id_map_get", "group", "tgt_write", "id_map_put", "ckpt_save")

    def __init__(self) -> None:
        self.totals: Dict[str, float] = {s: 0.0 for s in self.STAGES}
        self.batches = 0
        self._lock = threading.Lock()

    def add(self, stage: str, elapsed: float) -> None:
        with self._lock:
            self.totals[stage] += elapsed
            # batches are counted via mark_batch_end so a partial timing
            # (e.g. tgt_write only) doesn't inflate the count.

    def mark_batch_end(self) -> None:
        with self._lock:
            self.batches += 1

    def format_and_reset(self) -> Optional[str]:
        with self._lock:
            if self.batches == 0:
                return None
            parts = " ".join(f"{s}={self.totals[s]:.3f}s" for s in self.STAGES)
            line = f"stages {parts} (n={self.batches})"
            self.totals = {s: 0.0 for s in self.STAGES}
            self.batches = 0
            return line


class _Heartbeat:
    def __init__(
        self,
        label: str,
        total: int,
        interval_sec: float = HEARTBEAT_INTERVAL_SEC,
        state: Optional["_AutoTuneState"] = None,
        timings: Optional["_StageTimings"] = None,
        worker_states: Optional[List["_AutoTuneState"]] = None,
    ):
        self.label = label
        self.total = total
        self.interval = interval_sec
        self.start = time.time()
        self.last_emit = self.start
        self.done = 0
        self.state = state
        self.timings = timings
        # When set, multiple workers contribute to this heartbeat; the per-worker
        # auto-tune fields are surfaced as a compact suffix instead of `state`.
        self.worker_states = worker_states
        self._lock = threading.Lock()

    def tick(self, count: int) -> None:
        with self._lock:
            self.done += count
            now = time.time()
            if now - self.last_emit < self.interval:
                return
            done_snapshot = self.done
            self.last_emit = now
        elapsed = now - self.start
        rate = done_snapshot / elapsed if elapsed > 0 else 0
        pct = (done_snapshot / self.total * 100) if self.total else 0
        remaining = max(self.total - done_snapshot, 0)
        eta = remaining / rate if rate > 0 else 0
        line = (
            f"  [{self.label}] {done_snapshot:,}/{self.total:,} ({pct:.1f}%) "
            f"rate {rate:,.0f}/s elapsed {elapsed:.0f}s ETA {eta:.0f}s"
        )
        if self.worker_states is not None:
            # Compact per-worker view: "[w0:25,000 w1:20,000 w2:25,000]"
            parts = []
            for i, ws in enumerate(self.worker_states):
                if ws is None:
                    continue
                f = ws.heartbeat_fields()
                parts.append(f"w{i}:{f['current_batch_size']:,}")
            if parts:
                line += f" workers=[{' '.join(parts)}]"
        elif self.state is not None:
            f = self.state.heartbeat_fields()
            ceiling_str = "∞" if f["discovered_ceiling"] is None else f"{f['discovered_ceiling']:,}"
            last_rate = f"{f['last_batch_rate']:,.0f}/s" if f["last_batch_rate"] else "—"
            line += (
                f" last_batch_rate={last_rate}"
                f" batch={f['current_batch_size']:,}"
                f" ceiling={ceiling_str}"
                f" soft_ceiling={f['soft_ceiling']:,}"
            )
        print(line, flush=True)
        if self.timings is not None:
            stages_line = self.timings.format_and_reset()
            if stages_line:
                print(f"  [{self.label}] {stages_line}", flush=True)


def _bootstrap_initial(total_rows: int) -> int:
    """Initial batch size from total row count. Bootstrap heuristic only —
    overridden by the adaptive loop within ~5–10 batches."""
    if total_rows <= 0:
        return MIN_BATCH
    if total_rows < 5_000:
        return total_rows  # one-shot for tiny labels
    if total_rows < 100_000:
        return 5_000
    if total_rows < 1_000_000:
        return 10_000
    return 20_000


class _AutoTuneState:
    """Per-label/per-rel-type adaptive batch sizing state.

    See `tsk-20260502-aura-migration-auto-tune-batch.md` for the full algorithm
    (decisions table, edge cases, retry contract). Public surface:
      next_size()                 -> batch size to use for the next iteration
      record_batch(rows, elapsed) -> tick fast/slow counters; may halve/double
      record_oom(size, exc)       -> lower hard ceiling stickily; may raise InstanceTooSmall
      heartbeat_fields()          -> snapshot for _Heartbeat to surface
    """

    def __init__(self, label: str, total_rows: int, initial: Optional[int] = None) -> None:
        self.label = label
        self.current = initial if initial is not None else _bootstrap_initial(total_rows)
        self.ceiling: float = float("inf")
        self.soft_ceiling = self.current
        self.consec_slow = 0
        self.consec_fast = 0
        self.last_doubled_rate: Optional[float] = None
        self.last_slow_size: Optional[int] = None
        self.plateau_locked = False
        self.last_batch_rate: Optional[float] = None

    def next_size(self) -> int:
        return self.current

    def record_oom(self, size: int, exc: Exception) -> None:
        new_ceiling = size // 2
        if new_ceiling < MIN_BATCH:
            raise InstanceTooSmall(self.label, size, exc)
        self.ceiling = new_ceiling
        self.soft_ceiling = min(self.soft_ceiling, new_ceiling)
        self.current = int(self.ceiling)
        self.consec_slow = 0
        self.consec_fast = 0
        self.plateau_locked = True

    def record_batch(self, rows: int, elapsed: float) -> None:
        if rows <= 0 or elapsed <= 0:
            return
        rate = rows / elapsed
        self.last_batch_rate = rate
        size = self.current

        if elapsed > SLOW_SEC:
            self.consec_slow += 1
            self.consec_fast = 0
            if self.consec_slow >= SLOW_THRESHOLD:
                # Steady-state slow detection: same size went slow twice → sticky ceiling.
                if self.last_slow_size == size:
                    self.ceiling = max(MIN_BATCH, size // 2)
                    self.plateau_locked = True
                self.last_slow_size = size
                # Slow at-or-above soft_ceiling means soft_ceiling was claimed too high.
                if size >= self.soft_ceiling:
                    self.soft_ceiling = max(MIN_BATCH, size // 2)
                self.current = max(MIN_BATCH, size // 2)
                self.consec_slow = 0
                # NOTE: slow-halve does NOT set plateau_locked. The recovery path
                # in the FAST branch below will climb back up after the transient.
        elif elapsed < FAST_SEC:
            self.soft_ceiling = max(self.soft_ceiling, size)
            self.consec_fast += 1
            self.consec_slow = 0
            if self.consec_fast >= FAST_THRESHOLD:
                ceiling_int = None if self.ceiling == float("inf") else int(self.ceiling)
                # Recovery path: jump back to soft_ceiling without going through doubling.
                if size < self.soft_ceiling:
                    target = self.soft_ceiling if ceiling_int is None else min(self.soft_ceiling, ceiling_int)
                    self.current = target
                    self.consec_fast = 0
                    return
                if self.plateau_locked:
                    self.consec_fast = 0
                    return
                new_size = size * 2 if ceiling_int is None else min(size * 2, ceiling_int)
                if new_size <= size:
                    self.consec_fast = 0
                    return
                # Plateau check — single-sample today; median-of-3 deferred to v2.
                if self.last_doubled_rate is not None:
                    if rate < self.last_doubled_rate * PLATEAU_RATIO:
                        self.plateau_locked = True
                        self.consec_fast = 0
                        return
                self.last_doubled_rate = rate
                self.current = new_size
                self.consec_fast = 0
        else:
            self.consec_slow = 0
            self.consec_fast = 0

    def heartbeat_fields(self) -> Dict[str, Any]:
        return {
            "last_batch_rate": self.last_batch_rate,
            "current_batch_size": self.current,
            "discovered_ceiling": None if self.ceiling == float("inf") else int(self.ceiling),
            "soft_ceiling": self.soft_ceiling,
        }


# ── Checkpoint ─────────────────────────────────────────────────────────────────

def load_checkpoint(path: str) -> dict:
    if Path(path).exists():
        with open(path) as f:
            cp = json.load(f)
        version = cp.get("version")
        if version == 2 and CHECKPOINT_VERSION == 3:
            # v2 → v3 shim. v2 stored Phase 3 progress as a flat
            # `rel_type_last_eid: {<type>: <eid>}`; v3 stores per-worker progress
            # in `rel_type_workers: {<type>: [{worker_id, last_eid, upper_bound}]}`.
            # An in-flight v2 checkpoint maps cleanly to a single-worker v3 entry:
            # the saved cursor becomes worker 0's last_eid, no upper bound. This
            # preserves --resume across the upgrade without forcing customers
            # mid-migration to restart from scratch.
            cp["version"] = 3
            workers_map = cp.setdefault("rel_type_workers", {})
            migrated = 0
            for rel_type, last_eid in cp.get("rel_type_last_eid", {}).items():
                if last_eid and rel_type not in workers_map:
                    workers_map[rel_type] = [{
                        "worker_id": 0,
                        "last_eid": last_eid,
                        "upper_bound": None,
                    }]
                    migrated += 1
            print(
                f"  Migrated checkpoint v2 → v3 ({path}: "
                f"{migrated} in-flight rel type(s) preserved as single-worker)",
                file=sys.stderr,
            )
        elif version != CHECKPOINT_VERSION:
            print(
                f"  Incompatible checkpoint format (got version "
                f"{version!r}, expected {CHECKPOINT_VERSION}).\n"
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


_checkpoint_write_lock = threading.RLock()


def save_checkpoint(path: str, cp: dict) -> None:
    """Atomic checkpoint write. Phase 3 parallel workers write per-batch from
    multiple threads; the SIGINT handler also writes from the main thread —
    a torn write here corrupts JSON and makes --resume impossible. Module-level
    RLock serializes writers; the reentrant variant matters because the SIGINT
    handler runs on the main thread synchronously, and if a signal arrives while
    the main thread is already inside save_checkpoint (e.g. during Phase 1/2
    state persistence), a non-reentrant lock would deadlock the process.
    tempfile + os.replace makes the on-disk swap atomic against process kill.
    """
    with _checkpoint_write_lock:
        cp["updated_at"] = datetime.now(timezone.utc).isoformat()
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(cp, f, indent=2)
        os.replace(tmp_path, path)


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


# Estimated bytes per id_map entry in the Python in-memory dict — two interned
# strings (~80 B each on CPython 3.10+) plus dict-slot overhead. Empirical
# floor is closer to 200 B/entry for short elementIds; round up for safety
# when sizing EC2 RAM.
_ID_MAP_BYTES_PER_ENTRY = 256


def _load_id_map_into_memory(conn: sqlite3.Connection) -> Dict[str, str]:
    """Stream the full SQLite id_map into a Python dict. Phase 3 reads it via
    GIL-protected `dict.get` instead of issuing per-batch `SELECT IN (...)`
    queries, which under N parallel workers pays N× the SQLite shared-lock
    + page-cache contention cost. SQLite stays the durable store for Phase 2
    writes + `--resume` correctness; this is purely a Phase 3 read overlay
    rebuilt fresh each run.
    """
    t0 = time.time()
    print("\n  Loading id_map into memory (mode=memory) ...", flush=True)
    cur = conn.execute("SELECT source_eid, target_eid FROM id_map")
    in_mem: Dict[str, str] = {}
    progress_every = 1_000_000
    for source_eid, target_eid in cur:
        in_mem[source_eid] = target_eid
        if len(in_mem) % progress_every == 0:
            elapsed = time.time() - t0
            rate = len(in_mem) / elapsed if elapsed > 0 else 0
            print(f"    {len(in_mem):>10,} entries  ({rate:,.0f}/s)", flush=True)
    elapsed = time.time() - t0
    est_mb = len(in_mem) * _ID_MAP_BYTES_PER_ENTRY / (1024 ** 2)
    print(f"  ✓ {len(in_mem):,} entries in {elapsed:.1f}s "
          f"(~{est_mb:,.0f} MB)", flush=True)
    return in_mem


def _recommend_instance_type(node_count: int) -> str:
    """Map source node count → smallest EC2 instance with enough RAM for an
    in-memory id_map plus ~2 GB OS/Python overhead and ~500 MB headroom.
    Sizing assumes ~256 bytes per dict entry."""
    if node_count <= 1_000_000:    return "t3.medium (4 GB)"
    if node_count <= 5_000_000:    return "t3.large (8 GB)"
    if node_count <= 15_000_000:   return "t3.xlarge (16 GB)"
    if node_count <= 50_000_000:   return "t3.2xlarge (32 GB)"
    if node_count <= 100_000_000:  return "m6i.4xlarge (64 GB)"
    return "m6i.8xlarge (128 GB) or larger"


def _preflight_id_map_memory(node_count: int, mode: str) -> None:
    """Estimate id_map RAM for the given source-node count, compare to
    available memory on the host, and either print a recommendation or
    bail out with EC2 sizing guidance. Only enforces a hard fail in
    --id-map-mode=memory; sqlite mode prints info only.
    """
    est_bytes = node_count * _ID_MAP_BYTES_PER_ENTRY
    est_gb = est_bytes / (1024 ** 3)
    print(f"  id_map estimate : {node_count:,} nodes × ~{_ID_MAP_BYTES_PER_ENTRY} B "
          f"= ~{est_gb:.2f} GB")

    try:
        import psutil
        avail = psutil.virtual_memory().available
    except ImportError:
        print("  (psutil not installed — skipping host memory check)")
        return

    avail_gb = avail / (1024 ** 3)
    print(f"  host available  : ~{avail_gb:.2f} GB")

    if mode != "memory":
        return

    # Need id_map + ~500 MB headroom for batch buffers, driver, etc.
    needed = est_bytes + (500 * 1024 * 1024)
    if needed > avail:
        rec = _recommend_instance_type(node_count)
        print(
            f"\n  ✗ Insufficient memory for --id-map-mode=memory on this host.\n"
            f"    Required (id_map + 500 MB headroom): ~{needed / 1024**3:.2f} GB\n"
            f"    Available                           : ~{avail_gb:.2f} GB\n"
            f"    Recommended --instance-type         : {rec}\n"
            f"    Or pass --id-map-mode=sqlite to fall back to per-worker "
            f"SQLite reads (slower under parallel-rels but no in-memory cost).",
            file=sys.stderr,
        )
        sys.exit(4)


def id_map_get(conn: sqlite3.Connection, source_eids: List[str]) -> Dict[str, str]:
    """Return {source_eid: target_eid} for all source_eids present in the map.

    Chunks the lookup internally so callers can pass batches of any size —
    SQLite caps a single statement's variable count at 999 on older builds and
    32,766 on newer (Python 3.12+ / SQLite ≥ 3.32). With `--batch-size 40000`
    Phase 3 hands us up to ~80,000 unique eids per call (start + end of each
    rel), which blows past either limit. 5,000 keeps us safely under both
    while limiting round-trips.
    """
    if not source_eids:
        return {}
    out: Dict[str, str] = {}
    CHUNK = 5_000
    for i in range(0, len(source_eids), CHUNK):
        chunk = source_eids[i:i + CHUNK]
        placeholders = ",".join("?" * len(chunk))
        cur = conn.execute(
            f"SELECT source_eid, target_eid FROM id_map WHERE source_eid IN ({placeholders})",
            chunk,
        )
        for row in cur.fetchall():
            out[row[0]] = row[1]
    return out


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


def run_query(
    driver: Driver,
    cypher: str,
    params: Optional[Dict] = None,
    access_mode: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Execute a Cypher statement with retry on transient/connection errors.

    `access_mode="READ"` opens the session in read-only mode so Bolt routing
    can land it on a cluster follower — used by parallel Phase 3 workers to
    spread source reads across the source cluster's idle replicas.
    """
    session_kwargs = {"default_access_mode": access_mode} if access_mode else {}
    last_exc: Optional[Exception] = None
    for attempt, delay in enumerate((*_RETRY_DELAYS, None)):
        try:
            with driver.session(**session_kwargs) as session:
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


def _run_write_batch(
    driver: Driver, cypher: str, params: Optional[Dict] = None
) -> List[Dict[str, Any]]:
    """Execute a single write batch. Differs from run_query by NOT silently
    retrying OOM-coded TransientErrors — those propagate so the auto-tune
    handler can react. Non-OOM transients still retry with the same backoff
    schedule. See "Auto-tune Retry Contract" in the plan doc.
    """
    last_exc: Optional[Exception] = None
    for attempt, delay in enumerate((*_RETRY_DELAYS, None)):
        try:
            with driver.session() as session:
                result = session.run(cypher, params or {})
                return [record.data() for record in result]
        except TransientError as exc:
            if exc.code in OOM_CODES:
                raise
            if exc.code and "OutOfMemory" in exc.code:
                print(
                    f"  WARN: TransientError with OOM-shaped code {exc.code!r} not in OOM_CODES; "
                    f"treating as non-OOM transient — consider updating OOM_CODES",
                    file=sys.stderr,
                )
            last_exc = exc
            if delay is None:
                break
            print(
                f"  WARN: transient error (attempt {attempt + 1}/{len(_RETRY_DELAYS) + 1}), "
                f"retrying in {delay}s — {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            time.sleep(delay)
        except (ServiceUnavailable, SessionExpired, DatabaseUnavailable) as exc:
            last_exc = exc
            if delay is None:
                break
            print(
                f"  WARN: connection error (attempt {attempt + 1}/{len(_RETRY_DELAYS) + 1}), "
                f"retrying in {delay}s — {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


def _write_with_oom_retry(
    driver: Driver,
    cypher: str,
    batch_data: List[Dict[str, Any]],
    state: _AutoTuneState,
) -> Tuple[List[Dict[str, Any]], float, bool]:
    """Write `batch_data`; on OOM, lower the auto-tune ceiling and sub-divide.

    Returns (results, elapsed_sec, oom_occurred). Each OOM is a real signal —
    consecutive OOMs at retry are NOT coalesced. Raises InstanceTooSmall if
    halving would force the ceiling below MIN_BATCH.
    """
    if not batch_data:
        return [], 0.0, False

    t0 = time.time()
    try:
        results = _run_write_batch(driver, cypher, {"batch": batch_data})
        return results, time.time() - t0, False
    except TransientError as exc:
        if exc.code in OOM_CODES:
            elapsed_to_oom = time.time() - t0
            print(
                f"  auto-tune: OOM at batch={len(batch_data):,} "
                f"(code={exc.code}) — lowering ceiling and sub-dividing",
                file=sys.stderr,
                flush=True,
            )
            state.record_oom(len(batch_data), exc)  # may raise InstanceTooSmall
            mid = len(batch_data) // 2
            r1, e1, _ = _write_with_oom_retry(driver, cypher, batch_data[:mid], state)
            r2, e2, _ = _write_with_oom_retry(driver, cypher, batch_data[mid:], state)
            return r1 + r2, elapsed_to_oom + e1 + e2, True
        raise


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

    # LOOKUP indexes are auto-created by Neo4j; constraint backing-indexes are
    # auto-created by their owning constraint (filtering them avoids the
    # "EquivalentSchemaRuleAlreadyExists" warnings we'd otherwise see).
    indexes = run_query(
        src,
        "SHOW INDEXES YIELD type, createStatement, owningConstraint "
        "WHERE type IN ['RANGE', 'TEXT', 'POINT', 'VECTOR', 'FULLTEXT'] "
        "AND owningConstraint IS NULL",
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
    auto_tune: bool = False,
    profile_batches: bool = False,
) -> int:
    print("\n── Phase 2: Nodes ───────────────────────────────────────────────")

    labels = [r["label"] for r in run_query(src, "CALL db.labels() YIELD label")]
    print(f"  Labels: {', '.join(labels)}\n")

    migrated = 0
    pbar = tqdm(total=total_nodes, unit="nodes", disable=not sys.stdout.isatty()) if HAS_TQDM else None

    # When auto-tune is on, an explicitly-passed --batch-size acts as the initial hint;
    # default --batch-size lets the bootstrap heuristic pick a per-label initial.
    autotune_initial = batch_size if (auto_tune and batch_size != DEFAULT_BATCH_SIZE) else None

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

        state: Optional[_AutoTuneState] = (
            _AutoTuneState(label, label_total, initial=autotune_initial) if auto_tune else None
        )
        timings = _StageTimings() if profile_batches else None
        hb = _Heartbeat(label, label_total, state=state, timings=timings)

        if last_eid:
            done_so_far = count_query(
                src,
                f"MATCH (n:`{label}`) WHERE elementId(n) <= $last_eid RETURN count(n) AS cnt",
                {"last_eid": last_eid},
            )
            print(f"  [{label}] resuming after {done_so_far:,}/{label_total:,}")
            if pbar:
                pbar.update(done_so_far)
            hb.done = done_so_far

        while True:
            current_limit = state.next_size() if state is not None else batch_size
            t0 = time.perf_counter() if timings else 0.0
            records = run_query(
                src,
                f"MATCH (n:`{label}`) WHERE elementId(n) > $last_eid "
                "RETURN elementId(n) AS eid, labels(n) AS labels, properties(n) AS props "
                f"ORDER BY elementId(n) LIMIT {current_limit}",
                {"last_eid": last_eid},
            )
            if timings:
                timings.add("src_read", time.perf_counter() - t0)
            if not records:
                break

            # Bulk-check which source eids are already in the map (multi-label dedup)
            t0 = time.perf_counter() if timings else 0.0
            known = id_map_get(id_map, [r["eid"] for r in records])
            if timings:
                timings.add("id_map_get", time.perf_counter() - t0)

            t0 = time.perf_counter() if timings else 0.0
            new_records = [r for r in records if r["eid"] not in known]

            iteration_t0 = time.time() if state is not None else None
            iteration_oom = False

            groups: Dict[tuple, list] = {}
            if new_records:
                # Group by full sorted label tuple → one CREATE per unique label combo
                # avoids any dependency on APOC
                for r in new_records:
                    key = tuple(sorted(r["labels"]))
                    groups.setdefault(key, []).append(r)
            if timings:
                timings.add("group", time.perf_counter() - t0)

            for label_combo, group in groups.items():
                label_str = _label_clause(label_combo)
                cypher = (
                    f"UNWIND $batch AS row "
                    f"CREATE (n{label_str}) "
                    "SET n = row.props "
                    "RETURN row.source_eid AS source_eid, elementId(n) AS target_eid"
                )
                batch_data = [{"source_eid": r["eid"], "props": r["props"]} for r in group]
                t0 = time.perf_counter() if timings else 0.0
                if state is not None:
                    results, _, oom = _write_with_oom_retry(tgt, cypher, batch_data, state)
                    iteration_oom = iteration_oom or oom
                else:
                    results = run_query(tgt, cypher, {"batch": batch_data})
                if timings:
                    timings.add("tgt_write", time.perf_counter() - t0)
                t0 = time.perf_counter() if timings else 0.0
                id_map_put(id_map, [(r["source_eid"], r["target_eid"]) for r in results])
                if timings:
                    timings.add("id_map_put", time.perf_counter() - t0)

            if state is not None and not iteration_oom and iteration_t0 is not None:
                state.record_batch(len(new_records), time.time() - iteration_t0)

            count = len(records)
            last_eid = records[-1]["eid"]
            label_written += len(new_records)
            migrated += len(new_records)

            if pbar:
                pbar.update(len(new_records))

            cp["label_last_eid"][label] = last_eid
            t0 = time.perf_counter() if timings else 0.0
            save_checkpoint(cp_path, cp)
            if timings:
                timings.add("ckpt_save", time.perf_counter() - t0)
                timings.mark_batch_end()

            hb.tick(len(new_records))

            if count < current_limit:
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

def _discover_rel_partition(
    src: Driver, rel_type: str, count: int, n_workers: int
) -> List[Tuple[str, Optional[str]]]:
    """Return a list of (initial_last_eid, upper_bound_inclusive) tuples — one
    per worker. Each tuple defines the worker's slice of the rel type:
        WHERE elementId(r) > $last_eid AND elementId(r) <= $upper_bound

    Worker 0's `initial_last_eid` is "" (the existing unbounded-below sentinel).
    The last worker's `upper_bound` is None (unbounded above).

    Auto-clamps to a single full-range partition when:
      - n_workers <= 1
      - count < MIN_BATCH * n_workers (too small to bother partitioning)
      - SKIP/LIMIT discovery returned fewer boundaries than expected (rel
        type shrunk between count() and partition discovery)
    """
    if n_workers <= 1 or count < MIN_BATCH * n_workers:
        return [("", None)]

    boundaries: List[str] = []
    # boundary[k] = elementId at position (count*(k+1)/n_workers - 1), i.e. the
    # last row of worker k's slice — used as worker (k+1)'s `last_eid` so the
    # next worker's `> last_eid` cursor picks up exactly where worker k ended.
    for k in range(n_workers - 1):
        skip = (count * (k + 1)) // n_workers - 1
        if skip < 0:
            return [("", None)]
        rows = run_query(
            src,
            f"MATCH ()-[r:`{rel_type}`]->() WITH r ORDER BY elementId(r) "
            f"SKIP {skip} LIMIT 1 RETURN elementId(r) AS eid",
            access_mode="READ",
        )
        if not rows:
            return [("", None)]
        boundaries.append(rows[0]["eid"])

    out: List[Tuple[str, Optional[str]]] = []
    prev = ""
    for b in boundaries:
        out.append((prev, b))
        prev = b
    out.append((prev, None))
    return out


def _migrate_rel_worker(
    *,
    rel_type: str,
    src: Driver,
    tgt: Driver,
    id_map_path: str,
    in_mem_id_map: Optional[Dict[str, str]],
    cp: dict,
    cp_path: str,
    cp_lock: threading.Lock,
    worker_id: int,
    n_workers: int,
    initial_last_eid: str,
    upper_bound: Optional[str],
    batch_size: int,
    auto_tune: bool,
    autotune_initial: Optional[int],
    rel_total: int,
    state_holder: List[Optional["_AutoTuneState"]],
    timings: Optional[_StageTimings],
    hb: _Heartbeat,
    pbar: Optional[Any],
    pbar_lock: threading.Lock,
) -> int:
    """One worker for one rel type slice. Returns rels_written.

    Source reads use a READ-mode session so Bolt routing distributes them
    across cluster followers. Target writes go through the standard
    write-batch path (auto-tune retries on OOM; one transaction per batch).

    ID map lookup path (per Phase 3 batch):
      * `in_mem_id_map is not None` → GIL-protected `dict.get`. The dict was
        loaded once at Phase 3 start by the coordinator; workers share a
        read-only reference and never mutate. No locking required because
        `dict.get` is atomic in CPython and Phase 3 is read-only against
        the id_map.
      * `in_mem_id_map is None` → per-thread SQLite connection opened here
        (legacy/fallback path; selectable via --id-map-mode=sqlite). The
        sqlite3 default `check_same_thread=True` is satisfied because the
        connection is created and used entirely within this worker thread.
    """
    label_for_state = f"{rel_type}/w{worker_id}" if n_workers > 1 else rel_type
    state: Optional[_AutoTuneState] = (
        _AutoTuneState(label_for_state, rel_total, initial=autotune_initial) if auto_tune else None
    )
    state_holder[worker_id] = state
    # For n=1, surface the rich auto-tune heartbeat (last_batch_rate, ceiling,
    # soft_ceiling) — matches the pre-parallel single-worker output so
    # --parallel-rels=1 stays a clean regression baseline.
    if n_workers == 1:
        hb.state = state

    last_eid = initial_last_eid
    rel_written = 0
    id_map_conn: Optional[sqlite3.Connection] = (
        sqlite3.connect(id_map_path) if in_mem_id_map is None else None
    )

    try:
        while True:
            if _shutdown_event.is_set():
                return rel_written

            current_limit = state.next_size() if state is not None else batch_size

            t0 = time.perf_counter() if timings else 0.0
            if upper_bound is None:
                records = run_query(
                    src,
                    f"MATCH (a)-[r:`{rel_type}`]->(b) WHERE elementId(r) > $last_eid "
                    "RETURN elementId(r) AS r_eid, elementId(a) AS start_eid, "
                    "       elementId(b) AS end_eid, properties(r) AS props "
                    f"ORDER BY elementId(r) LIMIT {current_limit}",
                    {"last_eid": last_eid},
                    access_mode="READ",
                )
            else:
                records = run_query(
                    src,
                    f"MATCH (a)-[r:`{rel_type}`]->(b) "
                    "WHERE elementId(r) > $last_eid AND elementId(r) <= $upper_bound "
                    "RETURN elementId(r) AS r_eid, elementId(a) AS start_eid, "
                    "       elementId(b) AS end_eid, properties(r) AS props "
                    f"ORDER BY elementId(r) LIMIT {current_limit}",
                    {"last_eid": last_eid, "upper_bound": upper_bound},
                    access_mode="READ",
                )
            if timings:
                timings.add("src_read", time.perf_counter() - t0)
            if not records:
                break

            t0 = time.perf_counter() if timings else 0.0
            all_source_eids = list(
                {r["start_eid"] for r in records} | {r["end_eid"] for r in records}
            )
            if in_mem_id_map is not None:
                # Dict comprehension over GIL-protected dict.get — no SQLite
                # contention across workers. ~100 ns per lookup vs ~5–20 µs
                # per SQLite indexed read, plus eliminates the shared-lock
                # cost we measured at ~2.6× under N=2.
                eid_map = {
                    eid: in_mem_id_map[eid]
                    for eid in all_source_eids if eid in in_mem_id_map
                }
            else:
                eid_map = id_map_get(id_map_conn, all_source_eids)
            if timings:
                timings.add("id_map_get", time.perf_counter() - t0)

            t0 = time.perf_counter() if timings else 0.0
            batch_data = []
            missing = 0
            for r in records:
                t_start = eid_map.get(r["start_eid"])
                t_end = eid_map.get(r["end_eid"])
                if t_start and t_end:
                    batch_data.append({
                        "start_eid": t_start,
                        "end_eid": t_end,
                        "props": r["props"],
                    })
                else:
                    missing += 1
            if timings:
                timings.add("group", time.perf_counter() - t0)

            if missing:
                print(f"    WARN [{label_for_state}]: {missing} relationships skipped "
                      f"(node not in ID map)", flush=True)

            iteration_t0 = time.time() if state is not None else None
            iteration_oom = False

            if batch_data:
                # Canonical lock ordering: sort each batch by (min(start,end),
                # max(start,end)) before MATCHing the endpoints. This forces
                # every transaction across every parallel worker to acquire
                # EXCLUSIVE node locks in the same global order, eliminating
                # AB/BA deadlock cycles between workers whose batches touch
                # overlapping node endpoints. Pattern derived from a Neo4j
                # ingest-tuning case where canonical ordering removed an
                # observed class of intra- and inter-process deadlocks; we
                # observed the same class in twitch Run 3 (N=4) before this fix.
                cypher = (
                    "UNWIND $batch AS row "
                    "WITH row "
                    "ORDER BY "
                    "  CASE WHEN row.start_eid < row.end_eid THEN row.start_eid ELSE row.end_eid END, "
                    "  CASE WHEN row.start_eid < row.end_eid THEN row.end_eid ELSE row.start_eid END "
                    "MATCH (a) WHERE elementId(a) = row.start_eid "
                    "MATCH (b) WHERE elementId(b) = row.end_eid "
                    f"CREATE (a)-[r:`{rel_type}`]->(b) "
                    "SET r = row.props "
                    "RETURN count(r) AS created"
                )
                t0 = time.perf_counter() if timings else 0.0
                if state is not None:
                    _, _, oom = _write_with_oom_retry(tgt, cypher, batch_data, state)
                    iteration_oom = oom
                else:
                    run_query(tgt, cypher, {"batch": batch_data})
                if timings:
                    timings.add("tgt_write", time.perf_counter() - t0)

            if state is not None and not iteration_oom and iteration_t0 is not None:
                state.record_batch(len(batch_data), time.time() - iteration_t0)

            count = len(records)
            last_eid = records[-1]["r_eid"]
            rel_written += len(batch_data)

            if pbar:
                with pbar_lock:
                    pbar.update(count)

            t0 = time.perf_counter() if timings else 0.0
            with cp_lock:
                workers = cp.setdefault("rel_type_workers", {}).setdefault(rel_type, [])
                # Slot may have been pre-populated by the coordinator; ensure it exists.
                while len(workers) <= worker_id:
                    workers.append({"worker_id": len(workers), "last_eid": "", "upper_bound": None})
                workers[worker_id]["last_eid"] = last_eid
                workers[worker_id]["upper_bound"] = upper_bound
                save_checkpoint(cp_path, cp)
            if timings:
                timings.add("ckpt_save", time.perf_counter() - t0)
                timings.mark_batch_end()

            hb.tick(count)

            if count < current_limit:
                break
    finally:
        if id_map_conn is not None:
            id_map_conn.close()

    return rel_written


def migrate_relationships(
    src: Driver,
    tgt: Driver,
    id_map_path: str,
    cp: dict,
    cp_path: str,
    batch_size: int,
    total_rels: int,
    auto_tune: bool = False,
    profile_batches: bool = False,
    parallel_rels: int = 1,
    id_map_mode: str = "memory",
) -> int:
    print("\n── Phase 3: Relationships ───────────────────────────────────────")

    rel_types = [
        r["relationshipType"]
        for r in run_query(src, "CALL db.relationshipTypes() YIELD relationshipType")
    ]
    print(f"  Rel types: {', '.join(rel_types)}\n")
    if parallel_rels > 1:
        print(f"  Parallel workers per rel type: up to {parallel_rels} "
              f"(auto-clamped on small types)")
    print(f"  ID map mode: {id_map_mode}")

    # Build in-memory id_map (memory mode) once, reused by all workers across
    # all rel types. Skips SQLite-read contention that otherwise costs ~2.6×
    # per worker per batch under N>=2.
    in_mem_id_map: Optional[Dict[str, str]] = None
    if id_map_mode == "memory":
        with sqlite3.connect(id_map_path) as load_conn:
            in_mem_id_map = _load_id_map_into_memory(load_conn)

    migrated = 0
    pbar = tqdm(total=total_rels, unit="rels", disable=not sys.stdout.isatty()) if HAS_TQDM else None
    pbar_lock = threading.Lock()
    cp_lock = threading.Lock()

    autotune_initial = batch_size if (auto_tune and batch_size != DEFAULT_BATCH_SIZE) else None

    for rel_type in rel_types:
        if rel_type in cp["rel_types_complete"]:
            already = count_query(src, f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS cnt")
            if pbar:
                pbar.update(already)
            print(f"  [{rel_type}] already complete — skipping")
            continue

        rel_total = count_query(src, f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS cnt")
        timings = _StageTimings() if profile_batches else None

        # Resume path: workers state already in checkpoint.
        existing_workers = cp.get("rel_type_workers", {}).get(rel_type)
        if existing_workers:
            partition = [(w["last_eid"], w["upper_bound"]) for w in existing_workers]
            print(f"  [{rel_type}] resuming with {len(partition)} worker slot(s) from checkpoint")
        else:
            partition = _discover_rel_partition(src, rel_type, rel_total, parallel_rels)
            with cp_lock:
                cp.setdefault("rel_type_workers", {})[rel_type] = [
                    {"worker_id": i, "last_eid": p[0], "upper_bound": p[1]}
                    for i, p in enumerate(partition)
                ]
                save_checkpoint(cp_path, cp)
            if len(partition) > 1:
                print(f"  [{rel_type}] partitioned into {len(partition)} workers "
                      f"(rel_total={rel_total:,})")
            elif parallel_rels > 1:
                print(f"  [{rel_type}] count={rel_total:,} below parallel threshold; "
                      f"running single-worker")

        n = len(partition)
        state_holder: List[Optional[_AutoTuneState]] = [None] * n
        # n=1 keeps the original auto-tune-rich heartbeat format (last_batch_rate,
        # batch, ceiling, soft_ceiling) so --parallel-rels=1 stays bit-comparable
        # with main for regression testing. n>1 uses the compact per-worker view.
        hb = _Heartbeat(rel_type, rel_total, timings=timings,
                        worker_states=state_holder if n > 1 else None)
        # For n=1, hb.state is assigned by the worker once it constructs the
        # _AutoTuneState (worker runs in this thread; hb.state read is atomic).

        rel_written_total = 0

        if n == 1:
            rel_written_total = _migrate_rel_worker(
                rel_type=rel_type, src=src, tgt=tgt, id_map_path=id_map_path,
                in_mem_id_map=in_mem_id_map,
                cp=cp, cp_path=cp_path, cp_lock=cp_lock,
                worker_id=0, n_workers=1,
                initial_last_eid=partition[0][0], upper_bound=partition[0][1],
                batch_size=batch_size, auto_tune=auto_tune,
                autotune_initial=autotune_initial, rel_total=rel_total,
                state_holder=state_holder, timings=timings, hb=hb,
                pbar=pbar, pbar_lock=pbar_lock,
            )
        else:
            try:
                with ThreadPoolExecutor(max_workers=n, thread_name_prefix=f"rel-{rel_type}") as ex:
                    futures = {
                        ex.submit(
                            _migrate_rel_worker,
                            rel_type=rel_type, src=src, tgt=tgt, id_map_path=id_map_path,
                            in_mem_id_map=in_mem_id_map,
                            cp=cp, cp_path=cp_path, cp_lock=cp_lock,
                            worker_id=i, n_workers=n,
                            initial_last_eid=partition[i][0], upper_bound=partition[i][1],
                            batch_size=batch_size, auto_tune=auto_tune,
                            autotune_initial=autotune_initial, rel_total=rel_total,
                            state_holder=state_holder, timings=timings, hb=hb,
                            pbar=pbar, pbar_lock=pbar_lock,
                        ): i for i in range(n)
                    }
                    first_exc: Optional[BaseException] = None
                    for fut in as_completed(futures):
                        try:
                            rel_written_total += fut.result()
                        except BaseException as exc:
                            if first_exc is None:
                                first_exc = exc
                                _shutdown_event.set()
                                print(f"  [{rel_type}] worker {futures[fut]} raised "
                                      f"{type(exc).__name__}; signalling other workers to exit",
                                      file=sys.stderr, flush=True)
                    if first_exc is not None:
                        raise first_exc
            finally:
                # Always clear so a SIGINT- or exception-set event from this rel
                # type doesn't leak into the next iteration. The signal handler
                # is the only path that matters in practice (it sys.exits before
                # the loop continues), but the finally makes the contract explicit
                # and safe for any future caller that catches the re-raise.
                _shutdown_event.clear()

        migrated += rel_written_total

        with cp_lock:
            cp["rel_types_complete"].append(rel_type)
            cp.get("rel_type_workers", {}).pop(rel_type, None)
            cp["rel_type_last_eid"].pop(rel_type, None)
            save_checkpoint(cp_path, cp)
        print(f"  [{rel_type}] {rel_written_total:,} relationships written "
              f"(source total: {rel_total:,})")

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


_REMOTE_LOG_PATH = "/tmp/migrate.log"
_TAIL_POLL_INTERVAL_SEC = 5
_TAIL_INVOCATION_TIMEOUT_SEC = 10


def _tail_remote_log(ssm_client: Any, instance_id: str, byte_offset: int) -> str:
    """Send a short sidecar SSM command that does `tail -c +N` on the remote log.

    SSM's `StandardOutputContent` only surfaces output for COMPLETED commands.
    The main migration runs for many minutes (no surfaced stdout), but a sidecar
    `tail` finishes in milliseconds → its output is available immediately.
    Returns the new bytes since `byte_offset` (empty string if file missing or
    no new content yet).
    """
    cmd = f"tail -c +{byte_offset + 1} {_REMOTE_LOG_PATH} 2>/dev/null || true"
    resp = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [cmd]},
        TimeoutSeconds=30,
    )
    tail_id = resp["Command"]["CommandId"]
    deadline = time.time() + _TAIL_INVOCATION_TIMEOUT_SEC
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
    """Stream the EC2 migration's stdout in near-real time.

    Strategy: the main RunShellScript pipes migration output to /tmp/migrate.log
    via tee. We poll `get_command_invocation` for the main command's status, and
    in parallel send short sidecar `tail -c +N` SSM commands to read new bytes
    from the log file. Sidecar commands return their stdout immediately, which
    bypasses SSM's mid-run StandardOutputContent buffering on long-running
    commands.
    """
    time.sleep(5)  # give SSM time to register the main command + create the log
    bytes_seen = 0

    while True:
        # 1) Main-command status check.
        try:
            main = ssm_client.get_command_invocation(
                CommandId=command_id, InstanceId=instance_id
            )
            main_status = main["Status"]
        except ssm_client.exceptions.InvocationDoesNotExist:
            main_status = "InProgress"
            main = {"ResponseCode": -1, "StandardErrorContent": ""}

        # 2) Sidecar tail — print any new bytes since last poll.
        try:
            new_text = _tail_remote_log(ssm_client, instance_id, bytes_seen)
        except Exception as exc:  # noqa: BLE001 — never fail the migration on a tail glitch
            new_text = ""
            print(f"  (tail-sidecar failed: {exc})", file=sys.stderr, flush=True)

        if new_text:
            print(new_text, end="", flush=True)
            bytes_seen += len(new_text.encode("utf-8"))

        # 3) Terminal main status — final flush + return.
        if main_status in ("Success", "Failed", "Cancelled", "TimedOut"):
            # One last tail to catch anything written after our last poll.
            try:
                final = _tail_remote_log(ssm_client, instance_id, bytes_seen)
                if final:
                    print(final, end="", flush=True)
            except Exception:
                pass
            stderr = main.get("StandardErrorContent", "")
            if stderr:
                print(stderr, file=sys.stderr)
            return main.get("ResponseCode", -1)

        time.sleep(_TAIL_POLL_INTERVAL_SEC)


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
    if not args.auto_tune_batch_size:
        flags.append("--no-auto-tune-batch-size")
    if args.profile_batches:
        flags.append("--profile-batches")
    if args.parallel_rels != DEFAULT_PARALLEL_RELS:
        flags.append(f"--parallel-rels {args.parallel_rels}")
    if args.id_map_mode != "memory":
        flags.append(f"--id-map-mode {args.id_map_mode}")
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

    # Honor env-var/CLI passwords (matches the documented Usage in the module
    # docstring); only prompt interactively for whichever is missing.
    source_password = args.source_password or getpass.getpass("\nSource Aura password: ")
    target_password = args.target_password or getpass.getpass("Target Aura password: ")

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

        # Embed the local script via base64-of-gzip chunks instead of fetching from
        # GitHub. Embedding guarantees the EC2 runs exactly the local code (no
        # "main vs working tree" foot-gun, no supply-chain risk from an unpinned
        # raw URL). Gzip first because raw base64 of the script overflows SSM
        # SendCommand's 97 KB document+parameters limit once the file gets past
        # ~73 KB; gzip+base64 typically brings it back under 30 KB with plenty
        # of headroom for future growth.
        import gzip
        script_bytes = Path(__file__).resolve().read_bytes()
        script_b64 = base64.b64encode(gzip.compress(script_bytes)).decode()
        chunk_size = 4096
        chunks = [script_b64[i : i + chunk_size] for i in range(0, len(script_b64), chunk_size)]

        command_lines: List[str] = [
            "#!/bin/bash",
            "set -euo pipefail",
            # Amazon Linux 2023 ships python3 but not pip — install it first.
            "sudo dnf -y -q install python3-pip",
            "pip3 install -q neo4j tqdm psutil",
            "rm -f /tmp/migrate.b64 /tmp/migrate.py",
        ]
        for chunk in chunks:
            command_lines.append(f'printf "%s" "{chunk}" >> /tmp/migrate.b64')
        command_lines.extend([
            "base64 -d /tmp/migrate.b64 | gunzip > /tmp/migrate.py",
            "rm -f /tmp/migrate.b64",
            # Aura passwords go through env vars, not argv, so they don't leak via /proc.
            f'export SOURCE_AURA_PASSWORD=$(aws ssm get-parameter --region {region} --name "{src_pw_path}" --with-decryption --query Parameter.Value --output text)',
            f'export TARGET_AURA_PASSWORD=$(aws ssm get-parameter --region {region} --name "{tgt_pw_path}" --with-decryption --query Parameter.Value --output text)',
            # Tee to /tmp/migrate.log so the local-side `_stream_ssm_output` can
            # read deltas via short sidecar `tail -c +N` SSM commands. SSM's
            # StandardOutputContent doesn't surface output for long-running
            # commands until they terminate, so we route through a file instead.
            f'python3 -u /tmp/migrate.py --mode=local --source-uri="{args.source_uri}" --target-uri="{args.target_uri}" {passthrough} 2>&1 | tee /tmp/migrate.log',
        ])

        # SSM has TWO timeouts that both have to be set or the smaller one wins:
        #   - TimeoutSeconds (API-level, time the command may run before SSM kills it)
        #   - executionTimeout (document parameter on AWS-RunShellScript, default 3600s)
        # If executionTimeout is left at its default, the migration is SIGKILL'd at 1 hour
        # regardless of TimeoutSeconds. We set both to 12 hours — comfortably above the
        # observed ~75-minute runtime for a 4.7M-node / 10M-rel dataset over Aura PL,
        # leaving headroom for larger datasets or sustained Aura backpressure.
        cmd_resp = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={
                "commands": command_lines,
                "executionTimeout": ["43200"],  # 12 hours
            },
            TimeoutSeconds=43200,  # 12 hours — must match
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
                     help=f"Nodes/rels per transaction. With auto-tune ON (default), "
                          f"acts as the initial bootstrap value when explicitly set. "
                          f"With auto-tune OFF, acts as a fixed batch size. "
                          f"(default: {DEFAULT_BATCH_SIZE})")
    beh.add_argument("--auto-tune-batch-size", action=argparse.BooleanOptionalAction, default=True,
                     help="Adapt batch size at runtime based on per-batch wall-time, "
                          "OOMs, and throughput plateau (default: ON). "
                          "Use --no-auto-tune-batch-size to disable and run with a fixed --batch-size.")
    beh.add_argument("--profile-batches",  action="store_true",
                     help="Emit per-stage timing breakdown (src_read, id_map_get, group, "
                          "tgt_write, id_map_put, ckpt_save) at each heartbeat tick. "
                          "Off by default; flip on when investigating throughput.")
    beh.add_argument("--parallel-rels",    type=int, default=DEFAULT_PARALLEL_RELS,
                     metavar="N",
                     help=f"Phase 3 workers per rel type (default: {DEFAULT_PARALLEL_RELS}). "
                          "1 = sequential (pre-v3 behavior). Each worker opens its own "
                          "READ-mode source session so Bolt routing distributes reads "
                          "across the source cluster's followers. Auto-clamps to 1 for "
                          "rel types smaller than MIN_BATCH * N.")
    beh.add_argument("--id-map-mode",      choices=["memory", "sqlite"], default="memory",
                     help="Phase 3 id_map storage. 'memory' (default) loads the full "
                          "SQLite id_map into a Python dict at Phase 3 start so "
                          "parallel workers do GIL-protected lookups instead of "
                          "concurrent SQLite reads (eliminates the ~2.6× id_map_get "
                          "overhead measured under N>=2). 'sqlite' keeps per-worker "
                          "SQLite connections — use when the host doesn't have enough "
                          "RAM for the full id_map (~256 bytes per source node; see "
                          "README §Sizing).")
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
    ec2.add_argument("--instance-type",     default="t3.large",
                     help="EC2 instance type (default: t3.large — 8 GB, "
                          "covers ≤5M-node graphs in --id-map-mode=memory. "
                          "Bump to t3.xlarge / t3.2xlarge for larger graphs; "
                          "see README §Sizing).")
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
    if args.auto_tune_batch_size:
        if args.batch_size != DEFAULT_BATCH_SIZE:
            print(f"  Batch  : auto-tune ON (initial hint: {args.batch_size:,} rows/tx)")
        else:
            print(f"  Batch  : auto-tune ON (bootstrap from per-label counts)")
    else:
        print(f"  Batch  : {args.batch_size:,} rows/tx (auto-tune OFF)")
    if args.resume:
        print(f"  Resume : auto-tune state not persisted across runs; will re-discover ceilings")
    print("═" * 62)

    start_time = time.time()

    src = tgt = id_map = None
    try:
        # Open connections
        print("\n── Connecting ───────────────────────────────────────────────────")
        src = open_driver(args.source_uri, args.source_user, args.source_password, "source")
        tgt = open_driver(args.target_uri, args.target_user, args.target_password, "target")
        id_map = open_id_map(args.id_map_db)

        # Save checkpoint on Ctrl-C so --resume can pick up cleanly. Setting
        # _shutdown_event first asks any running Phase 3 workers to exit between
        # batches; in-flight batches finish (or fail their retries) before the
        # workers return.
        def _handle_sigint(sig, frame):
            _shutdown_event.set()
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

        # Memory sizing recommendation / hard fail for in-memory id_map mode.
        # Done after preflight so we have an authoritative source node count.
        _preflight_id_map_memory(counts["src_nodes"], args.id_map_mode)

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
                auto_tune=args.auto_tune_batch_size,
                profile_batches=args.profile_batches,
            )

        # ── Phase 3 ───────────────────────────────────────────────────────────
        if cp.get("rels_done"):
            print("\n── Phase 3: Relationships — SKIPPED (already complete)")
        else:
            migrate_relationships(
                src, tgt, args.id_map_db, cp, args.checkpoint_file,
                args.batch_size, counts["src_rels"],
                auto_tune=args.auto_tune_batch_size,
                profile_batches=args.profile_batches,
                parallel_rels=args.parallel_rels,
                id_map_mode=args.id_map_mode,
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

    except InstanceTooSmall as exc:
        print(f"\n{'═' * 62}", file=sys.stderr)
        print(f"  ✗ {exc}", file=sys.stderr)
        print(f"  Checkpoint preserved at: {args.checkpoint_file}", file=sys.stderr)
        print(f"  Re-run with --resume after upgrading the target Aura tier.", file=sys.stderr)
        print(f"{'═' * 62}", file=sys.stderr)
        sys.exit(3)  # distinct from generic exit 1 — wrappers can recognize "tier too small"
    finally:
        if src: src.close()
        if tgt: tgt.close()
        if id_map: id_map.close()


if __name__ == "__main__":
    main()
