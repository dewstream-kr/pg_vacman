# SPDX-License-Identifier: GPL-3.0-or-later
# pg_vacman.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pg_vacman v1.0: PostgreSQL maintenance manager (multi-database VACUUM/ANALYZE runner)

This tool scans tables across one or more databases, decides whether to run
ANALYZE / VACUUM (ANALYZE) / VACUUM (FREEZE, ANALYZE) / VACUUM (FULL, ANALYZE),
and executes the plan with configurable concurrency and safety limits.

Key features
- Multi-database loop (targets discovered from pg_database or explicitly listed)
- Table filtering (include/exclude by schema and object patterns)
- Threshold-based decisions (size, dead ratio, last analyze/vacuum age, freeze age)
- Optional "force" policy for specific tables
- Safety controls (lock_timeout, statement_timeout, advisory lock to avoid overlap)
- Graceful stop (Ctrl+C once) and immediate stop (Ctrl+C twice / SIGTERM)
- JSON run report + optional Slack/Telegram notifications

Dependencies
- Python 3.7+:
  pip3 install "psycopg[binary]" pyyaml requests
- Python 3.6:
  pip3 install dataclasses psycopg2-binary pyyaml requests

Notes
- Uses psycopg3 if available; falls back to psycopg2.
- Requires sufficient privileges to run maintenance commands.
"""

import argparse
import datetime as dt
import fnmatch
import json
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

try:
    import psycopg
    from psycopg.rows import dict_row

    psycopg3_available = True
except Exception:
    psycopg3_available = False
    import psycopg2
    import psycopg2.extras


# Supported actions produced by the planner.
allowed_actions = {
    "ANALYZE",
    "VACUUM_ANALYZE",
    "VACUUM_FREEZE_ANALYZE",
    "VACUUM_FULL_ANALYZE",
}

# Stop flags:
# - graceful_stop_event: stop scheduling new work; let in-flight tasks finish
# - immediate_stop_event: attempt to cancel in-flight queries
graceful_stop_event = threading.Event()
immediate_stop_event = threading.Event()

# Track active DB connections (for cancellation on immediate stop).
active_conns_lock = threading.Lock()
active_conns: Dict[int, Any] = {}  # backend_pid -> connection object

sigint_count_lock = threading.Lock()
sigint_count = 0


@dataclass(frozen=True)
class table_candidate:
    """A table with statistics required for maintenance decision-making."""
    schemaname: str
    relname: str
    relid: int
    total_size_bytes: int
    n_live_tup: int
    n_dead_tup: int
    dead_ratio: float
    last_vacuum: Optional[dt.datetime]
    last_autovacuum: Optional[dt.datetime]
    last_analyze: Optional[dt.datetime]
    last_autoanalyze: Optional[dt.datetime]
    freeze_age: int


@dataclass(frozen=True)
class action_task:
    """A planned maintenance action for a specific table in a specific database."""
    dbname: str
    candidate: table_candidate
    action: str
    reason: str
    decision: Dict[str, Any]


def mask_password(pw: Any) -> str:
    """Mask password strings for logs."""
    if pw is None:
        return ""
    s = str(pw)
    return "********" if s else ""


def format_conn_info(db_cfg: Dict[str, Any]) -> str:
    """Return a sanitized connection string for error logs."""
    host = db_cfg.get("host", "")
    port = db_cfg.get("port", "")
    dbname = db_cfg.get("dbname", "")
    user = db_cfg.get("user", "")
    password = mask_password(db_cfg.get("password", ""))
    timeout = db_cfg.get("connect_timeout_sec", 5)
    app = db_cfg.get("application_name", "")
    return (
        f"host={host} port={port} dbname={dbname} user={user} password={password} "
        f"connect_timeout_sec={timeout} application_name={app}"
    )


def log_connect_error(context: str, db_cfg: Dict[str, Any], exc: Exception) -> None:
    """Log connection failures with safe details and quick diagnostic hints."""
    logging.error("db_connect_failed context=%s conn=%s", context, format_conn_info(db_cfg))
    logging.error("db_connect_failed error=%s", str(exc))

    host = db_cfg.get("host", "")
    port = db_cfg.get("port", "")
    dbname = db_cfg.get("dbname", "")
    user = db_cfg.get("user", "")

    logging.error(
        "db_connect_failed hints: "
        "1) network reachability (firewall / security group) "
        "2) pg_hba.conf rule "
        "3) user/password "
        "4) dbname exists / datallowconn "
        "5) TLS requirement"
    )
    logging.error(
        "db_connect_failed quick_check: "
        f'psql "host={host} port={port} dbname={dbname} user={user}"'
    )


class pg_client:
    """Context-managed PostgreSQL client that supports psycopg3 and psycopg2."""

    def __init__(self, cfg: Dict[str, Any], context: str = "unknown"):
        self.cfg = cfg
        self.context = context
        self.conn = None

    def __enter__(self) -> "pg_client":
        db_cfg = self.cfg["db"]
        try:
            if psycopg3_available:
                self.conn = psycopg.connect(
                    host=db_cfg["host"],
                    port=db_cfg["port"],
                    dbname=db_cfg["dbname"],
                    user=db_cfg["user"],
                    password=db_cfg["password"],
                    connect_timeout=db_cfg.get("connect_timeout_sec", 5),
                    application_name=db_cfg.get("application_name", "pg_vacman"),
                    row_factory=dict_row,
                )
                self.conn.autocommit = True
            else:
                self.conn = psycopg2.connect(
                    host=db_cfg["host"],
                    port=db_cfg["port"],
                    dbname=db_cfg["dbname"],
                    user=db_cfg["user"],
                    password=db_cfg["password"],
                    connect_timeout=db_cfg.get("connect_timeout_sec", 5),
                    application_name=db_cfg.get("application_name", "pg_vacman"),
                )
                self.conn.autocommit = True
            return self
        except Exception as e:
            log_connect_error(self.context, db_cfg, e)
            raise

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self.conn:
                self.conn.close()
        finally:
            self.conn = None

    def execute(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        if psycopg3_available:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
        else:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)

    def fetchall(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
        if psycopg3_available:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        else:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())

    def fetchone(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> Optional[Dict[str, Any]]:
        rows = self.fetchall(sql, params)
        return rows[0] if rows else None


def setup_logging(level_name: str) -> None:
    """Configure root logger."""
    level_map = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR}
    level = level_map.get((level_name or "info").strip().lower(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(path: str) -> Dict[str, Any]:
    """Load YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def now_in_tz(timezone_name: str) -> dt.datetime:
    """Return current time in the given timezone if zoneinfo is available."""
    try:
        from zoneinfo import ZoneInfo

        return dt.datetime.now(tz=ZoneInfo(timezone_name))
    except Exception:
        return dt.datetime.now()


def bytes_to_mb(size_bytes: int) -> float:
    return size_bytes / (1024 * 1024)


def quote_ident(name: str) -> str:
    """Safely quote an SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def fqtn(schema: str, table: str) -> str:
    """Fully-qualified table name with quoted identifiers."""
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def parse_hhmm(hhmm: str) -> Tuple[int, int]:
    hh, mm = hhmm.split(":")
    return int(hh), int(mm)


def in_time_window(local_now: dt.datetime, start_hhmm: str, end_hhmm: str) -> bool:
    """Return True if local_now is within [start, end], handling midnight wrap."""
    start_h, start_m = parse_hhmm(start_hhmm)
    end_h, end_m = parse_hhmm(end_hhmm)

    start = local_now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    end = local_now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)

    if end <= start:
        return local_now >= start or local_now <= end
    return start <= local_now <= end


def age_hours(ts: Optional[dt.datetime], now: dt.datetime) -> Optional[float]:
    """Return age in hours between now and ts, preserving tz awareness when possible."""
    if ts is None:
        return None

    if ts.tzinfo and not now.tzinfo:
        now = now.replace(tzinfo=ts.tzinfo)
    if not ts.tzinfo and now.tzinfo:
        ts = ts.replace(tzinfo=now.tzinfo)

    return (now - ts).total_seconds() / 3600.0


def slack_notify(webhook_url: str, text: str) -> None:
    """Send a Slack message via incoming webhook."""
    if not webhook_url:
        return
    try:
        requests.post(webhook_url, json={"text": text}, timeout=5)
    except Exception as e:
        logging.warning("slack_notify failed: %s", e)


def telegram_notify(bot_token: str, chat_id: str, text: str) -> None:
    """Send a Telegram message via bot API."""
    if not bot_token or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": text}, timeout=5)
    except Exception as e:
        logging.warning("telegram_notify failed: %s", e)


def cfg_for_db(cfg: Dict[str, Any], dbname: str) -> Dict[str, Any]:
    """Clone config and override dbname."""
    new_cfg = dict(cfg)
    new_db = dict(cfg.get("db", {}))
    new_db["dbname"] = dbname
    new_cfg["db"] = new_db
    return new_cfg


def cfg_with_application_suffix(cfg: Dict[str, Any], suffix: str) -> Dict[str, Any]:
    """Clone config and append a suffix to application_name (helps tracing in pg_stat_activity)."""
    new_cfg = dict(cfg)
    db_cfg = dict(new_cfg.get("db", {}))

    base = str(db_cfg.get("application_name", "pg_vacman")).strip()
    if not base:
        base = "pg_vacman"

    db_cfg["application_name"] = f"{base}:{suffix}"
    new_cfg["db"] = db_cfg
    return new_cfg


def try_advisory_lock(pg: pg_client, key: int) -> bool:
    """Best-effort singleton lock to prevent overlapping runs."""
    row = pg.fetchone("select pg_try_advisory_lock(%s) as ok;", (key,))
    return bool(row and row["ok"])


def release_advisory_lock(pg: pg_client, key: int) -> None:
    pg.execute("select pg_advisory_unlock(%s);", (key,))


def is_primary(pg: pg_client) -> bool:
    """Return True when connected to a primary (not in recovery)."""
    row = pg.fetchone("select pg_is_in_recovery() as in_recovery;")
    if not row:
        return True
    return not bool(row["in_recovery"])


def apply_session_settings(pg: pg_client, cfg: Dict[str, Any]) -> None:
    """Apply per-session safety settings for maintenance operations."""
    limits_cfg = cfg.get("limits", {}) or {}

    lock_timeout_ms = int(limits_cfg.get("lock_timeout_ms", 2000) or 2000)
    statement_timeout_ms = int(limits_cfg.get("per_table_statement_timeout_sec", 1800) or 1800) * 1000

    pg.execute(f"set lock_timeout = '{lock_timeout_ms}ms';")
    pg.execute(f"set statement_timeout = '{statement_timeout_ms}ms';")

    vacuum_cost_delay_ms = int(limits_cfg.get("vacuum_cost_delay_ms", 0) or 0)
    vacuum_cost_limit = int(limits_cfg.get("vacuum_cost_limit", 0) or 0)

    if vacuum_cost_delay_ms > 0:
        pg.execute(f"set vacuum_cost_delay = '{vacuum_cost_delay_ms}ms';")
    if vacuum_cost_limit > 0:
        pg.execute(f"set vacuum_cost_limit = {vacuum_cost_limit};")


def normalize_object_patterns(cfg: Dict[str, Any]) -> None:
    """
    Normalize legacy include_tables/exclude_tables into include_objects/exclude_objects.

    Supported pattern formats:
    - "db_pattern:schema.table_pattern"
    - "schema.table_pattern" (db_pattern defaults to "*")
    """
    filters_cfg = cfg.get("filters", {}) or {}

    include_objects = list(filters_cfg.get("include_objects") or [])
    exclude_objects = list(filters_cfg.get("exclude_objects") or [])

    include_tables = list(filters_cfg.get("include_tables") or [])
    exclude_tables = list(filters_cfg.get("exclude_tables") or [])

    def to_object_pat(p: str) -> str:
        if ":" in p:
            return p
        return f"*:{p}"

    include_objects.extend([to_object_pat(p) for p in include_tables])
    exclude_objects.extend([to_object_pat(p) for p in exclude_tables])

    filters_cfg["include_objects"] = include_objects
    filters_cfg["exclude_objects"] = exclude_objects
    cfg["filters"] = filters_cfg


def match_object_pattern(dbname: str, schema: str, table: str, pattern: str) -> bool:
    """Match db/schema.table against a pattern using fnmatch semantics."""
    if ":" in pattern:
        db_pat, obj_pat = pattern.split(":", 1)
    else:
        db_pat, obj_pat = "*", pattern

    target_obj = f"{schema}.{table}"
    return fnmatch.fnmatchcase(dbname, db_pat) and fnmatch.fnmatchcase(target_obj, obj_pat)


def object_filter_decision(dbname: str, schema: str, table: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply include/exclude filtering.

    Behavior:
    - If include_objects is empty: allow all unless excluded
    - If include_objects is not empty: allow only matching includes, unless excluded
    """
    filters_cfg = cfg.get("filters", {}) or {}
    include_objects = filters_cfg.get("include_objects") or []
    exclude_objects = filters_cfg.get("exclude_objects") or []

    mode = "allow_all" if not include_objects else "whitelist"

    for pat in exclude_objects:
        if match_object_pattern(dbname, schema, table, pat):
            return {
                "allowed": False,
                "mode": mode,
                "exclude_hit": pat,
                "include_hit": None,
                "reason": "excluded_by_exclude_objects",
            }

    if not include_objects:
        return {
            "allowed": True,
            "mode": mode,
            "exclude_hit": None,
            "include_hit": None,
            "reason": "include_empty_allow_all",
        }

    for pat in include_objects:
        if match_object_pattern(dbname, schema, table, pat):
            return {
                "allowed": True,
                "mode": mode,
                "exclude_hit": None,
                "include_hit": pat,
                "reason": "include_matched",
            }

    return {
        "allowed": False,
        "mode": mode,
        "exclude_hit": None,
        "include_hit": None,
        "reason": "include_miss_in_whitelist_mode",
    }


def get_force_decision(cfg: Dict[str, Any], dbname: str, schema: str, table: str) -> Dict[str, Any]:
    """Return forced action decision if force policy matches the table."""
    force_cfg = cfg.get("force", {}) or {}
    if not bool(force_cfg.get("enabled", False)):
        return {"matched": False, "pattern": None, "action": None, "reason": None}

    default_action = str(force_cfg.get("default_action", "ANALYZE")).upper().strip()
    if default_action not in allowed_actions:
        default_action = "ANALYZE"

    items = force_cfg.get("tables") or []
    for item in items:
        if isinstance(item, str):
            pattern = item.strip()
            action = default_action
        elif isinstance(item, dict):
            pattern = str(item.get("pattern", "")).strip()
            action = str(item.get("action", default_action)).upper().strip()
            if action not in allowed_actions:
                action = default_action
        else:
            continue

        if not pattern:
            continue

        if match_object_pattern(dbname, schema, table, pattern):
            return {"matched": True, "pattern": pattern, "action": action, "reason": "force_matched"}

    return {"matched": False, "pattern": None, "action": None, "reason": None}


def list_target_databases(pg: pg_client, cfg: Dict[str, Any]) -> List[str]:
    """Build the target database list based on targets configuration."""
    targets_cfg = cfg.get("targets", {}) or {}

    include_databases = targets_cfg.get("include_databases") or []
    exclude_databases = set(targets_cfg.get("exclude_databases") or [])
    exclude_templates = bool(targets_cfg.get("exclude_templates", True))
    require_allow_conn = bool(targets_cfg.get("require_allow_conn", True))

    if include_databases:
        rows = pg.fetchall("select datname from pg_database where datname = any(%s);", (include_databases,))
        found = {r["datname"] for r in rows}
        missing = [d for d in include_databases if d not in found]
        if missing:
            logging.warning("include_databases contains missing DB(s): %s", missing)
        return [d for d in include_databases if d in found and d not in exclude_databases]

    where = ["1=1"]
    params: List[Any] = []

    if exclude_templates:
        where.append("datistemplate = false")
    if require_allow_conn:
        where.append("datallowconn = true")
    if exclude_databases:
        where.append("datname <> all(%s)")
        params.append(list(exclude_databases))

    sql = f"""
    select datname
    from pg_database
    where {" and ".join(where)}
    order by datname;
    """
    rows = pg.fetchall(sql, tuple(params) if params else None)
    return [r["datname"] for r in rows]


def build_filters_snapshot(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Snapshot filters into the run report (useful for debugging)."""
    filters_cfg = cfg.get("filters", {}) or {}
    return {
        "include_schemas": list(filters_cfg.get("include_schemas") or []),
        "exclude_schemas": list(filters_cfg.get("exclude_schemas") or []),
        "include_objects": list(filters_cfg.get("include_objects") or []),
        "exclude_objects": list(filters_cfg.get("exclude_objects") or []),
        "include_tables_legacy": list(filters_cfg.get("include_tables") or []),
        "exclude_tables_legacy": list(filters_cfg.get("exclude_tables") or []),
    }


def build_candidates_with_skips(
    pg: pg_client,
    cfg: Dict[str, Any],
    dbname: str,
    json_detail_level: str,
    json_max_skips_per_db: int,
) -> Tuple[List[table_candidate], List[Dict[str, Any]], int]:
    """
    Fetch table stats and build candidates list.

    If json_detail_level is "verbose", also collect filtered-out objects
    (limited by json_max_skips_per_db to avoid oversized reports).
    """
    filters_cfg = cfg.get("filters", {}) or {}
    include_schemas = filters_cfg.get("include_schemas") or []
    exclude_schemas = set(filters_cfg.get("exclude_schemas") or [])

    where_parts = [
        "n.nspname not in ('pg_catalog','information_schema')",
        "c.relkind = 'r'",
    ]
    params: List[Any] = []

    for s in exclude_schemas:
        where_parts.append("n.nspname <> %s")
        params.append(s)

    if include_schemas:
        where_parts.append("n.nspname = any(%s)")
        params.append(include_schemas)

    sql = f"""
    select
        n.nspname as schemaname,
        c.relname as relname,
        c.oid     as relid,
        pg_total_relation_size(c.oid) as total_size_bytes,
        coalesce(st.n_live_tup, 0) as n_live_tup,
        coalesce(st.n_dead_tup, 0) as n_dead_tup,
        st.last_vacuum,
        st.last_autovacuum,
        st.last_analyze,
        st.last_autoanalyze,
        age(c.relfrozenxid) as freeze_age
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    left join pg_stat_user_tables st on st.relid = c.oid
    where {" and ".join(where_parts)}
    order by pg_total_relation_size(c.oid) desc;
    """

    rows = pg.fetchall(sql, tuple(params) if params else None)

    candidates: List[table_candidate] = []
    skipped_objects: List[Dict[str, Any]] = []
    skipped_overflow = 0

    for r in rows:
        schema = r["schemaname"]
        table = r["relname"]

        flt = object_filter_decision(dbname, schema, table, cfg)
        allowed = bool(flt.get("allowed", False))

        if not allowed:
            if json_detail_level == "verbose":
                if len(skipped_objects) < max(0, json_max_skips_per_db):
                    live = int(r["n_live_tup"] or 0)
                    dead = int(r["n_dead_tup"] or 0)
                    denom = live + dead
                    dead_ratio = (dead / denom) if denom > 0 else 0.0
                    skipped_objects.append(
                        {
                            "db": dbname,
                            "table": f"{schema}.{table}",
                            "filter": flt,
                            "stats": {
                                "size_mb": round(bytes_to_mb(int(r["total_size_bytes"] or 0)), 2),
                                "n_live_tup": live,
                                "n_dead_tup": dead,
                                "dead_ratio": round(float(dead_ratio), 8),
                            },
                        }
                    )
                else:
                    skipped_overflow += 1
            continue

        live = int(r["n_live_tup"] or 0)
        dead = int(r["n_dead_tup"] or 0)
        denom = live + dead
        dead_ratio = (dead / denom) if denom > 0 else 0.0

        freeze_age_val = r.get("freeze_age", 0)
        try:
            freeze_age_int = int(freeze_age_val or 0)
        except Exception:
            freeze_age_int = 0

        candidates.append(
            table_candidate(
                schemaname=schema,
                relname=table,
                relid=int(r["relid"]),
                total_size_bytes=int(r["total_size_bytes"] or 0),
                n_live_tup=live,
                n_dead_tup=dead,
                dead_ratio=float(dead_ratio),
                last_vacuum=r["last_vacuum"],
                last_autovacuum=r["last_autovacuum"],
                last_analyze=r["last_analyze"],
                last_autoanalyze=r["last_autoanalyze"],
                freeze_age=freeze_age_int,
            )
        )

    return candidates, skipped_objects, skipped_overflow


def decide_action_verbose(c: table_candidate, cfg: Dict[str, Any], local_now: dt.datetime) -> Tuple[str, str, Dict[str, Any]]:
    """
    Decide the maintenance action for a candidate and return:
    (action, human_reason, verbose_decision_dict)

    Action decision order:
    1) Skip if table is smaller than min_table_size_mb
    2) Optionally choose VACUUM FULL within a specified time window
    3) If dead ratio is high and vacuum/analyze is stale -> VACUUM (FREEZE?) ANALYZE
    4) If analyze is stale -> ANALYZE
    5) Otherwise -> SKIP
    """
    thresholds_cfg = cfg.get("thresholds", {}) or {}

    min_table_size_mb = float(thresholds_cfg.get("min_table_size_mb", 256))
    min_dead_ratio = float(thresholds_cfg.get("min_dead_ratio", 0.2))
    max_analyze_age_hours = float(thresholds_cfg.get("max_analyze_age_hours", 24))
    max_last_vacuum_age_hours = float(thresholds_cfg.get("max_last_vacuum_age_hours", 48))
    freeze_age_threshold = int(thresholds_cfg.get("freeze_age_threshold", 1500000000))

    size_mb = bytes_to_mb(c.total_size_bytes)

    last_analyze = c.last_autoanalyze or c.last_analyze
    analyze_age = age_hours(last_analyze, local_now)

    last_vacuum = c.last_autovacuum or c.last_vacuum
    vacuum_age = age_hours(last_vacuum, local_now)

    needs_analyze = (analyze_age is None) or (analyze_age >= max_analyze_age_hours)
    needs_vacuum = (vacuum_age is None) or (vacuum_age >= max_last_vacuum_age_hours)
    needs_freeze = c.freeze_age >= freeze_age_threshold

    th_dec: Dict[str, Any] = {
        "evaluated": True,
        "inputs": {
            "size_mb": float(round(size_mb, 4)),
            "dead_ratio": float(round(c.dead_ratio, 8)),
            "freeze_age": int(c.freeze_age),
            "last_analyze": last_analyze.isoformat() if last_analyze else None,
            "last_vacuum": last_vacuum.isoformat() if last_vacuum else None,
            "analyze_age_hours": float(round(analyze_age, 4)) if analyze_age is not None else None,
            "vacuum_age_hours": float(round(vacuum_age, 4)) if vacuum_age is not None else None,
        },
        "params": {
            "min_table_size_mb": min_table_size_mb,
            "min_dead_ratio": min_dead_ratio,
            "max_analyze_age_hours": max_analyze_age_hours,
            "max_last_vacuum_age_hours": max_last_vacuum_age_hours,
            "freeze_age_threshold": freeze_age_threshold,
        },
        "flags": {
            "needs_analyze": bool(needs_analyze),
            "needs_vacuum": bool(needs_vacuum),
            "needs_freeze": bool(needs_freeze),
        },
        "rule": "",
    }

    if size_mb < min_table_size_mb:
        th_dec["rule"] = "size_mb < min_table_size_mb => SKIP"
        return "SKIP", f"size_mb<{min_table_size_mb}", th_dec

    vacuum_full_cfg = thresholds_cfg.get("vacuum_full", {}) or {}
    if bool(vacuum_full_cfg.get("enabled", False)):
        full_min_dead_ratio = float(vacuum_full_cfg.get("min_dead_ratio", 0.6))
        full_min_size_mb = float(vacuum_full_cfg.get("min_table_size_mb", 2048))
        start_hhmm = str(vacuum_full_cfg.get("start", "01:00"))
        end_hhmm = str(vacuum_full_cfg.get("end", "05:00"))

        if c.dead_ratio >= full_min_dead_ratio and size_mb >= full_min_size_mb and in_time_window(local_now, start_hhmm, end_hhmm):
            th_dec["rule"] = "vacuum_full enabled and conditions met => VACUUM_FULL_ANALYZE"
            return "VACUUM_FULL_ANALYZE", "VACUUM FULL conditions and time window satisfied", th_dec

    if c.dead_ratio >= min_dead_ratio and (needs_vacuum or needs_analyze):
        if needs_freeze:
            th_dec["rule"] = "dead_ratio>=min_dead_ratio and needs_freeze => VACUUM_FREEZE_ANALYZE"
            return "VACUUM_FREEZE_ANALYZE", "dead_ratio and freeze_age threshold satisfied", th_dec
        th_dec["rule"] = "dead_ratio>=min_dead_ratio and (needs_vacuum or needs_analyze) => VACUUM_ANALYZE"
        return "VACUUM_ANALYZE", "dead_ratio high and vacuum/analyze refresh needed", th_dec

    if needs_analyze:
        th_dec["rule"] = "needs_analyze => ANALYZE"
        return "ANALYZE", "analyze refresh needed", th_dec

    th_dec["rule"] = "no conditions met => SKIP"
    return "SKIP", "thresholds not met", th_dec


def make_maintenance_sql(schema: str, table: str, action: str) -> str:
    """Build SQL for the demonstrated maintenance action."""
    target = fqtn(schema, table)

    if action == "ANALYZE":
        return f"analyze {target};"
    if action == "VACUUM_ANALYZE":
        return f"vacuum (analyze) {target};"
    if action == "VACUUM_FREEZE_ANALYZE":
        return f"vacuum (freeze, analyze) {target};"
    if action == "VACUUM_FULL_ANALYZE":
        return f"vacuum (full, analyze) {target};"

    return ""


def slice_plans_by_limits(
    plans: List[action_task],
    max_tables_per_db: int,
    max_actions_global: int,
    global_actions_count: int,
) -> Tuple[List[action_task], bool]:
    """Apply per-db and global action limits; return (trimmed_plans, global_limit_reached)."""
    if max_tables_per_db > 0:
        plans = plans[:max_tables_per_db]

    if max_actions_global > 0:
        remaining = max_actions_global - global_actions_count
        if remaining <= 0:
            return [], True
        plans = plans[:remaining]

    return plans, False


def register_active_conn(pg: pg_client) -> Optional[int]:
    """Register a connection by backend pid so it can be cancelled on immediate stop."""
    try:
        row = pg.fetchone("select pg_backend_pid() as pid;")
        if not row:
            return None
        pid = int(row["pid"])
        with active_conns_lock:
            active_conns[pid] = pg.conn
        return pid
    except Exception:
        return None


def unregister_active_conn(pid: Optional[int]) -> None:
    if pid is None:
        return
    with active_conns_lock:
        active_conns.pop(pid, None)


def cancel_all_active_conns() -> None:
    """Best-effort cancellation/close of all registered active connections."""
    with active_conns_lock:
        items = list(active_conns.items())

    if not items:
        return

    logging.warning("immediate_stop: attempting to cancel %d active connections", len(items))

    for pid, conn in items:
        try:
            if conn is None:
                continue
            try:
                conn.cancel()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
        except Exception:
            pass


def vacuum_worker(
    base_cfg: Dict[str, Any],
    task: action_task,
    dry_run: bool,
    global_sem: threading.Semaphore,
    json_detail_level: str,
) -> Dict[str, Any]:
    """
    Execute a single maintenance task.
    Concurrency is constrained by global_sem (global_parallel_limit).
    """
    c = task.candidate
    sql = make_maintenance_sql(c.schemaname, c.relname, task.action)

    entry: Dict[str, Any] = {
        "db": task.dbname,
        "table": f"{c.schemaname}.{c.relname}",
        "action": task.action,
        "reason": task.reason,
        "ok": False,
        "skipped_by_stop": False,
    }

    if json_detail_level == "verbose":
        entry["decision"] = task.decision

    if not sql:
        entry["ok"] = True
        entry["reason"] = "no_sql"
        return entry

    if immediate_stop_event.is_set():
        entry["skipped_by_stop"] = True
        entry["reason"] = "immediate_stop"
        return entry

    if graceful_stop_event.is_set():
        entry["skipped_by_stop"] = True
        entry["reason"] = "graceful_stop"
        return entry

    global_sem.acquire()
    pid: Optional[int] = None
    started_at = dt.datetime.now().isoformat()
    t0 = time.time()

    entry["execution"] = {
        "dry_run": dry_run,
        "sql": sql,
        "backend_pid": None,
        "started_at": started_at,
        "ended_at": None,
        "elapsed_ms": None,
        "error": None,
    }

    try:
        if immediate_stop_event.is_set():
            entry["skipped_by_stop"] = True
            entry["reason"] = "immediate_stop"
            return entry

        db_cfg = cfg_for_db(base_cfg, task.dbname)
        db_cfg = cfg_with_application_suffix(db_cfg, "worker")

        with pg_client(db_cfg, context=f"worker:{task.dbname}") as pg:
            apply_session_settings(pg, base_cfg)
            pid = register_active_conn(pg)
            entry["execution"]["backend_pid"] = pid

            if dry_run:
                logging.info("[DRY-RUN] db=%s %s", task.dbname, sql)
                entry["ok"] = True
                return entry

            if immediate_stop_event.is_set():
                entry["skipped_by_stop"] = True
                entry["reason"] = "immediate_stop"
                return entry

            logging.info("exec db=%s %s", task.dbname, sql)
            pg.execute(sql)
            entry["ok"] = True
            return entry

    except Exception as e:
        logging.exception(
            "worker failed db=%s table=%s.%s action=%s",
            task.dbname,
            c.schemaname,
            c.relname,
            task.action,
        )
        entry["execution"]["error"] = str(e)
        entry["ok"] = False
        return entry

    finally:
        entry["execution"]["ended_at"] = dt.datetime.now().isoformat()
        entry["execution"]["elapsed_ms"] = int((time.time() - t0) * 1000)
        unregister_active_conn(pid)
        global_sem.release()


def resolve_run_mode(args: argparse.Namespace, cfg: Dict[str, Any]) -> bool:
    """Return True for dry-run, False for apply."""
    if args.apply:
        return False
    if args.dry_run:
        return True
    return bool(cfg.get("run", {}).get("dry_run_default", True))


def install_signal_handlers() -> None:
    """
    Install signal handlers:
    - SIGINT: 1st => graceful stop; 2nd => immediate stop + cancel
    - SIGTERM: immediate stop + cancel
    """
    def on_sigint(signum, frame):
        global sigint_count
        with sigint_count_lock:
            sigint_count += 1
            count = sigint_count

        if count == 1:
            graceful_stop_event.set()
            logging.warning(
                "SIGINT received: graceful stop (stop scheduling new tasks). "
                "Press Ctrl+C again for immediate stop."
            )
            return

        immediate_stop_event.set()
        graceful_stop_event.set()
        logging.warning("SIGINT received again: immediate stop (attempting to cancel running queries).")
        threading.Thread(target=cancel_all_active_conns, daemon=True).start()

    def on_sigterm(signum, frame):
        immediate_stop_event.set()
        graceful_stop_event.set()
        logging.warning("SIGTERM received: immediate stop (attempting to cancel running queries).")
        threading.Thread(target=cancel_all_active_conns, daemon=True).start()

    signal.signal(signal.SIGINT, on_sigint)
    signal.signal(signal.SIGTERM, on_sigterm)


def ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def default_json_out_path(run_cfg: Dict[str, Any], local_now: dt.datetime) -> str:
    """Build default JSON output path based on config prefix and current time."""
    out_dir = str(run_cfg.get("json_out_dir", ".") or ".")
    prefix = str(run_cfg.get("json_out_prefix", "run") or "run")
    ensure_dir(out_dir)
    ts = local_now.strftime("%Y%m%d_%H%M%S")
    return os.path.join(out_dir, f"{prefix}_{ts}.json")


def _one_line(s: Any, max_len: int = 220) -> str:
    """Render text into a single line and truncate to max_len."""
    if s is None:
        return ""
    t = str(s).replace("\n", " ").replace("\r", " ").strip()
    if len(t) > max_len:
        return t[: max_len - 3] + "..."
    return t


def _fmt_elapsed_ms(ms: Any) -> str:
    """Format elapsed milliseconds into human-friendly string."""
    try:
        v = int(ms)
    except Exception:
        return ""
    if v < 1000:
        return f"{v}ms"
    return f"{round(v / 1000.0, 2)}s"


def build_notify_text_summary_and_details(
    run_summary: Dict[str, Any],
    json_path: str,
    timezone_name: str,
    max_actions_per_db: int,
    include_sql: bool,
) -> str:
    """
    Build a compact, readable notification message.

    Structure:
    - Header: global summary
    - Per DB: planned/ok/fail or skip reason
    - Per DB details: up to max_actions_per_db actions (failures first)
    - Footer: JSON output path if available
    """
    ts = run_summary.get("ts", "")
    dry_run = bool(run_summary.get("dry_run", True))
    mode = "DRY-RUN" if dry_run else "APPLY"

    g = run_summary.get("global", {}) or {}
    planned = g.get("planned_actions", 0)
    ok = g.get("executed_ok", 0)
    fail = g.get("executed_fail", 0)
    skipped_dbs = g.get("skipped_dbs", 0)

    aborted = bool(run_summary.get("aborted", False))
    abort_mode = run_summary.get("abort_mode", "None")

    parallel_db = g.get("parallel_tables_per_db", "")
    parallel_global = g.get("global_parallel_limit", "")
    detail = run_summary.get("detail_level", "")

    lines: List[str] = []
    lines.append(f"[pg_vacman] {mode} {ts} ({timezone_name})")
    lines.append(
        f"planned={planned} ok={ok} fail={fail} skipped_dbs={skipped_dbs} aborted={aborted}({abort_mode})"
    )
    lines.append(f"detail={detail} parallel(db={parallel_db}/global={parallel_global})")

    db_results = run_summary.get("db_results", []) or []
    for db in db_results:
        dbname = db.get("db", "")
        db_planned = db.get("planned", 0)
        db_ok = db.get("ok", 0)
        db_fail = db.get("fail", 0)
        skipped_reason = db.get("skipped_reason", "")

        lines.append("")
        if skipped_reason:
            lines.append(f"DB: {dbname} skipped_reason={skipped_reason}")
            continue

        lines.append(f"DB: {dbname} planned={db_planned} ok={db_ok} fail={db_fail}")

        actions = db.get("actions", []) or []

        # Sort: FAIL first, then OK, then SKIP (stop-skipped)
        def _sort_key(a: Dict[str, Any]) -> Tuple[int, str]:
            if a.get("skipped_by_stop"):
                grp = 2
            elif a.get("ok"):
                grp = 1
            else:
                grp = 0
            return (grp, str(a.get("table", "")))

        actions_sorted = sorted(actions, key=_sort_key)

        lines.append("status | action | table | elapsed | message" + (" | sql" if include_sql else ""))

        shown = 0
        for a in actions_sorted:
            if shown >= max_actions_per_db:
                break

            status = "OK" if a.get("ok") else "FAIL"
            if a.get("skipped_by_stop"):
                status = "SKIP"

            action = a.get("action", "")
            table = a.get("table", "")
            ex = a.get("execution", {}) or {}
            elapsed = _fmt_elapsed_ms(ex.get("elapsed_ms"))

            if status in ("OK", "SKIP"):
                msg = _one_line(a.get("reason", ""), 180)
            else:
                err = _one_line(ex.get("error"), 180)
                msg = err if err else _one_line(a.get("reason", ""), 180)

            row = f"{status} | {action} | {table} | {elapsed} | {msg}".rstrip()

            if include_sql:
                sql = _one_line(ex.get("sql", ""), 180)
                row = f"{row} | {sql}".rstrip()

            lines.append(row)
            shown += 1

        remaining = len(actions_sorted) - shown
        if remaining > 0:
            lines.append(f"... and {remaining} more actions in {dbname}")

    if json_path:
        lines.append("")
        lines.append(f"run.json: {json_path}")

    text = "\n".join(lines).strip()
    return f"```\n{text}\n```"


def main() -> int:
    ap = argparse.ArgumentParser(description="PostgreSQL maintenance manager (multi-db loop)")
    ap.add_argument("--config", required=True, help="config.yaml path")
    ap.add_argument("--dry-run", action="store_true", help="do not execute, only print plan")
    ap.add_argument("--apply", action="store_true", help="execute actions")
    ap.add_argument("--json-out", default="", help="write run result json to path (if empty, auto path is used)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    run_cfg = cfg.get("run", {}) or {}

    setup_logging(str(run_cfg.get("log_level", "info")))
    install_signal_handlers()

    normalize_object_patterns(cfg)

    dry_run = resolve_run_mode(args, cfg)

    targets_cfg = cfg.get("targets", {}) or {}
    limits_cfg = cfg.get("limits", {}) or {}
    notify_cfg = cfg.get("notify", {}) or {}

    timezone_name = str(run_cfg.get("timezone", "Asia/Seoul"))
    local_now = now_in_tz(timezone_name)

    advisory_lock_key = int(run_cfg.get("advisory_lock_key", 90421001))
    primary_only = bool(targets_cfg.get("primary_only", False))

    max_databases_per_run = int(targets_cfg.get("max_databases_per_run", 0) or 0)

    max_tables_per_db = int(limits_cfg.get("max_tables_per_db", 0) or 0)
    max_actions_global = int(limits_cfg.get("max_actions_global", 0) or 0)

    parallel_tables_per_db = int(limits_cfg.get("parallel_tables_per_db", 2) or 2)
    if parallel_tables_per_db < 1:
        parallel_tables_per_db = 1

    global_parallel_limit = int(limits_cfg.get("global_parallel_limit", parallel_tables_per_db) or parallel_tables_per_db)
    if global_parallel_limit < 1:
        global_parallel_limit = 1

    sleep_between_tables_sec = float(limits_cfg.get("sleep_between_tables_sec", 0) or 0)
    sleep_between_databases_sec = float(limits_cfg.get("sleep_between_databases_sec", 0) or 0)

    json_detail_level = str(run_cfg.get("json_detail_level", "verbose")).strip().lower()
    if json_detail_level not in ("basic", "verbose"):
        json_detail_level = "verbose"

    json_max_skips_per_db = int(run_cfg.get("json_max_skips_per_db", 50) or 50)
    if json_max_skips_per_db < 0:
        json_max_skips_per_db = 50

    json_auto_save = bool(run_cfg.get("json_auto_save", True))

    notify_max_actions_per_db = int(run_cfg.get("notify_max_actions_per_db", 30) or 30)
    if notify_max_actions_per_db < 1:
        notify_max_actions_per_db = 30

    notify_include_sql = bool(run_cfg.get("notify_include_sql", False))

    global_sem = threading.Semaphore(global_parallel_limit)

    filters_snapshot = build_filters_snapshot(cfg)

    run_summary: Dict[str, Any] = {
        "ts": local_now.isoformat(),
        "dry_run": dry_run,
        "aborted": False,
        "abort_mode": "None",
        "detail_level": json_detail_level,
        "filters_snapshot": filters_snapshot if json_detail_level == "verbose" else {},
        "global": {
            "planned_actions": 0,
            "executed_ok": 0,
            "executed_fail": 0,
            "skipped_dbs": 0,
            "parallel_tables_per_db": parallel_tables_per_db,
            "global_parallel_limit": global_parallel_limit,
            "max_tables_per_db": max_tables_per_db,
            "max_actions_global": max_actions_global,
            "json_max_skips_per_db": json_max_skips_per_db,
        },
        "db_results": [],
    }

    cfg_control = cfg_with_application_suffix(cfg, "control")

    # If we cannot connect to the control DB, fail fast.
    try:
        with pg_client(cfg_control, context="control") as pg_ctrl:
            if not try_advisory_lock(pg_ctrl, advisory_lock_key):
                msg = f"[pg_vacman] already running (advisory_lock_key={advisory_lock_key})"
                logging.warning(msg)
                slack_notify(notify_cfg.get("slack_webhook_url", ""), msg)
                telegram_notify(notify_cfg.get("telegram_bot_token", ""), notify_cfg.get("telegram_chat_id", ""), msg)
                return 2

            try:
                db_list = list_target_databases(pg_ctrl, cfg)
                if max_databases_per_run > 0:
                    db_list = db_list[:max_databases_per_run]
                logging.info("target_databases=%d %s", len(db_list), db_list)
            except Exception as e:
                logging.exception("failed to list databases: %s", e)
                release_advisory_lock(pg_ctrl, advisory_lock_key)
                return 1
    except Exception:
        return 3

    global_actions_count = 0
    json_path = ""

    try:
        for dbname in db_list:
            if immediate_stop_event.is_set():
                run_summary["aborted"] = True
                run_summary["abort_mode"] = "immediate"
                break

            if graceful_stop_event.is_set():
                run_summary["aborted"] = True
                run_summary["abort_mode"] = "graceful"
                break

            logging.info("db_start=%s", dbname)

            db_report: Dict[str, Any] = {
                "db": dbname,
                "planned": 0,
                "ok": 0,
                "fail": 0,
                "actions": [],
            }

            if json_detail_level == "verbose":
                db_report["filters_snapshot"] = filters_snapshot
                db_report["skipped_objects"] = []
                db_report["skipped_objects_overflow"] = 0

            candidates: List[table_candidate] = []

            try:
                db_cfg_ctrl = cfg_for_db(cfg_control, dbname)
                with pg_client(db_cfg_ctrl, context=f"candidate:{dbname}") as pg_db_ctrl:
                    if primary_only and not is_primary(pg_db_ctrl):
                        db_report["skipped_reason"] = "standby"
                        run_summary["global"]["skipped_dbs"] += 1
                        run_summary["db_results"].append(db_report)
                        logging.info("db_skip=%s reason=standby", dbname)
                        if sleep_between_databases_sec > 0:
                            time.sleep(sleep_between_databases_sec)
                        continue

                    candidates, skipped_objects, skipped_overflow = build_candidates_with_skips(
                        pg=pg_db_ctrl,
                        cfg=cfg,
                        dbname=dbname,
                        json_detail_level=json_detail_level,
                        json_max_skips_per_db=json_max_skips_per_db,
                    )

                if json_detail_level == "verbose":
                    db_report["skipped_objects"] = skipped_objects
                    db_report["skipped_objects_overflow"] = skipped_overflow

            except Exception as e:
                logging.exception("db_candidate_fetch_failed=%s err=%s", dbname, e)
                db_report["skipped_reason"] = f"candidate_fetch_error: {e}"
                run_summary["global"]["skipped_dbs"] += 1
                run_summary["db_results"].append(db_report)
                if sleep_between_databases_sec > 0:
                    time.sleep(sleep_between_databases_sec)
                continue

            plans: List[action_task] = []

            for c in candidates:
                schema = c.schemaname
                table = c.relname

                flt = object_filter_decision(dbname, schema, table, cfg)
                force_dec = get_force_decision(cfg, dbname, schema, table)

                if force_dec["matched"]:
                    action = force_dec["action"]
                    reason = "force policy applied"
                    th_dec = {"evaluated": False}
                    source = "force"
                else:
                    action, reason, th_dec = decide_action_verbose(c, cfg, local_now)
                    source = "thresholds"

                decision = {
                    "filter": flt,
                    "force": force_dec,
                    "thresholds": th_dec,
                    "final_action": {
                        "action": action,
                        "reason": reason,
                        "source": source,
                    },
                }

                if action != "SKIP":
                    plans.append(action_task(dbname=dbname, candidate=c, action=action, reason=reason, decision=decision))

            plans, global_limit_reached = slice_plans_by_limits(
                plans=plans,
                max_tables_per_db=max_tables_per_db,
                max_actions_global=max_actions_global,
                global_actions_count=global_actions_count,
            )

            if global_limit_reached:
                db_report["skipped_reason"] = "global_limit_reached"
                run_summary["global"]["skipped_dbs"] += 1
                run_summary["db_results"].append(db_report)
                logging.info("db_skip=%s reason=global_limit_reached", dbname)
                break

            db_report["planned"] = len(plans)
            run_summary["global"]["planned_actions"] += len(plans)
            global_actions_count += len(plans)

            logging.info(
                "db_plan=%s candidates=%d planned=%d skips(filter)=%d(+%d overflow) dry_run=%s",
                dbname,
                len(candidates),
                len(plans),
                len(db_report.get("skipped_objects", [])) if json_detail_level == "verbose" else 0,
                int(db_report.get("skipped_objects_overflow", 0)) if json_detail_level == "verbose" else 0,
                dry_run,
            )

            if not plans:
                run_summary["db_results"].append(db_report)
                logging.info("db_end=%s planned=0", dbname)
                if sleep_between_databases_sec > 0:
                    time.sleep(sleep_between_databases_sec)
                continue

            if parallel_tables_per_db == 1:
                for t in plans:
                    if immediate_stop_event.is_set():
                        run_summary["aborted"] = True
                        run_summary["abort_mode"] = "immediate"
                        break

                    res = vacuum_worker(cfg, t, dry_run, global_sem, json_detail_level)
                    db_report["actions"].append(res)

                    if res.get("ok"):
                        db_report["ok"] += 1
                        run_summary["global"]["executed_ok"] += 1
                    else:
                        db_report["fail"] += 1
                        run_summary["global"]["executed_fail"] += 1

                    if sleep_between_tables_sec > 0:
                        time.sleep(sleep_between_tables_sec)
            else:
                with ThreadPoolExecutor(max_workers=parallel_tables_per_db) as ex:
                    futures = []
                    for t in plans:
                        if immediate_stop_event.is_set() or graceful_stop_event.is_set():
                            break
                        futures.append(ex.submit(vacuum_worker, cfg, t, dry_run, global_sem, json_detail_level))

                    for fut in as_completed(futures):
                        res = fut.result()
                        db_report["actions"].append(res)

                        if res.get("ok"):
                            db_report["ok"] += 1
                            run_summary["global"]["executed_ok"] += 1
                        else:
                            db_report["fail"] += 1
                            run_summary["global"]["executed_fail"] += 1

                        if sleep_between_tables_sec > 0:
                            time.sleep(sleep_between_tables_sec)

            run_summary["db_results"].append(db_report)
            logging.info(
                "db_end=%s planned=%d ok=%d fail=%d",
                dbname,
                db_report["planned"],
                db_report["ok"],
                db_report["fail"],
            )

            if sleep_between_databases_sec > 0:
                time.sleep(sleep_between_databases_sec)

        # JSON output policy:
        # - If --json-out is provided: write there
        # - Else: if json_auto_save is true, write to default path
        json_path = args.json_out.strip()
        if not json_path and json_auto_save:
            json_path = default_json_out_path(run_cfg, local_now)

        if json_path:
            try:
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(run_summary, f, ensure_ascii=False, indent=2)
                logging.info("json_saved path=%s", json_path)
            except Exception as e:
                logging.error("json_save_failed path=%s error=%s", json_path, str(e))

        notify_text = build_notify_text_summary_and_details(
            run_summary=run_summary,
            json_path=json_path,
            timezone_name=timezone_name,
            max_actions_per_db=notify_max_actions_per_db,
            include_sql=notify_include_sql,
        )

        slack_notify(notify_cfg.get("slack_webhook_url", ""), notify_text)
        telegram_notify(notify_cfg.get("telegram_bot_token", ""), notify_cfg.get("telegram_chat_id", ""), notify_text)

        if immediate_stop_event.is_set():
            cancel_all_active_conns()

        return 0

    finally:
        try:
            with pg_client(cfg_control, context="control_unlock") as pg_ctrl2:
                release_advisory_lock(pg_ctrl2, advisory_lock_key)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
