#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# SPDX-License-Identifier: GPL-3.0-or-later
"""
pg_vacman v1.1 (py3.6-compatible): PostgreSQL maintenance manager (multi-database VACUUM/ANALYZE runner)

Target legacy environments:
- RHEL/CentOS 7.x + Python 3.6
- Older OpenSSL/libssl environments where urllib3 v2 can break

Compatibility choices:
- psycopg2 only
- No required dependency on requests (optional; falls back to stdlib urllib)
- No dataclasses (uses typing.NamedTuple)

New v1.1 features (also in this py3.6 build):
- Precheck: skip when autovacuum/vacuum/analyze is already running on the same table (best-effort)
- VACUUM FULL guardrail (allowlist + downgrade/skip)
- Retry/backoff on retryable failures (lock timeout, statement timeout, deadlock, etc.)
- Run report counts: OK / FAIL / SKIP separated

Dependencies (recommended pins for Python 3.6):
- psycopg2-binary==2.9.6
- PyYAML==6.0.1
- requests==2.27.1 (optional; safe choice to avoid urllib3 v2 on old OpenSSL)

Example:
  python3.6 -m pip install -r requirements_py36.txt
"""

import argparse
import datetime as dt
import fnmatch
import json
import logging
import os
import random
import signal
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple, NamedTuple

# Optional HTTP client (may fail to import if urllib3/OpenSSL mismatch exists)
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore


# Supported actions produced by the planner.
allowed_actions = {
    "ANALYZE",
    "VACUUM_ANALYZE",
    "VACUUM_FREEZE_ANALYZE",
    "VACUUM_FULL_ANALYZE",
}

allowed_statuses = {"OK", "SKIP", "FAIL"}

# Stop flags:
# - graceful_stop_event: stop scheduling new work; let in-flight tasks finish
# - immediate_stop_event: attempt to cancel in-flight queries
graceful_stop_event = threading.Event()
immediate_stop_event = threading.Event()

# Track active DB connections (for cancellation on immediate stop).
active_conns_lock = threading.Lock()
active_conns = {}  # type: Dict[int, Any]  # backend_pid -> connection object

sigint_count_lock = threading.Lock()
sigint_count = 0


class table_candidate(NamedTuple):
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
    avg_row_width_bytes: float
    estimated_dead_bytes: float
    estimated_dead_ratio: float


class action_task(NamedTuple):
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
        "host={host} port={port} dbname={dbname} user={user} password={password} "
        "connect_timeout_sec={timeout} application_name={app}"
    ).format(
        host=host, port=port, dbname=dbname, user=user, password=password, timeout=timeout, app=app
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
        'psql "host={host} port={port} dbname={dbname} user={user}"'.format(
            host=host, port=port, dbname=dbname, user=user
        )
    )


class pg_client(object):
    """Context-managed PostgreSQL client (psycopg2 only for Python 3.6 compatibility)."""

    def __init__(self, cfg: Dict[str, Any], context: str = "unknown"):
        self.cfg = cfg
        self.context = context
        self.conn = None

    def __enter__(self) -> "pg_client":
        db_cfg = self.cfg["db"]
        try:
            if psycopg2 is None:
                raise RuntimeError("PostgreSQL driver is required. Install psycopg2-binary==2.9.6 for Python 3.6.")
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
        with self.conn.cursor() as cur:
            cur.execute(sql, params)

    def fetchall(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
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
    """
    Load configuration.

    - If file ends with .json: parse JSON (no extra deps)
    - Otherwise: parse YAML via PyYAML (PyYAML==6.0.1 recommended for py3.6)
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    if path.lower().endswith(".json"):
        return json.loads(raw)

    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "PyYAML is required to read YAML config. Install PyYAML==6.0.1 (recommended for Python 3.6). "
            "Original error: {err}".format(err=e)
        )

    return yaml.safe_load(raw)


def validate_config(cfg: Dict[str, Any]) -> None:
    """Validate configuration values that can be checked before connecting."""
    errors = []  # type: List[str]

    def _require_section(name: str) -> Dict[str, Any]:
        section = cfg.get(name, {}) or {}
        if not isinstance(section, dict):
            errors.append("{name} must be a mapping".format(name=name))
            return {}
        return section

    def _int_at(section_name: str, section: Dict[str, Any], key: str, minimum: Optional[int] = None) -> None:
        if key not in section:
            return
        try:
            value = int(section.get(key))
            if minimum is not None and value < minimum:
                errors.append("{section}.{key} must be >= {minimum}".format(section=section_name, key=key, minimum=minimum))
        except Exception:
            errors.append("{section}.{key} must be an integer".format(section=section_name, key=key))

    def _float_at(section_name: str, section: Dict[str, Any], key: str, minimum: Optional[float] = None) -> None:
        if key not in section:
            return
        try:
            value = float(section.get(key))
            if minimum is not None and value < minimum:
                errors.append("{section}.{key} must be >= {minimum}".format(section=section_name, key=key, minimum=minimum))
        except Exception:
            errors.append("{section}.{key} must be a number".format(section=section_name, key=key))

    def _hhmm(path_name: str, value: Any) -> None:
        try:
            hour, minute = parse_hhmm(str(value))
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                errors.append("{path} must be HH:MM in 00:00..23:59".format(path=path_name))
        except Exception:
            errors.append("{path} must be HH:MM".format(path=path_name))

    db_cfg = _require_section("db")
    for key in ("host", "port", "dbname", "user"):
        if key not in db_cfg or str(db_cfg.get(key) or "").strip() == "":
            errors.append("db.{key} is required".format(key=key))
    _int_at("db", db_cfg, "port", 1)
    try:
        if "port" in db_cfg and int(db_cfg.get("port")) > 65535:
            errors.append("db.port must be <= 65535")
    except Exception:
        pass
    _int_at("db", db_cfg, "connect_timeout_sec", 1)

    targets_cfg = _require_section("targets")
    _int_at("targets", targets_cfg, "max_databases_per_run", 0)

    run_cfg = _require_section("run")
    _int_at("run", run_cfg, "advisory_lock_key")
    _int_at("run", run_cfg, "json_max_skips_per_db", 0)
    _int_at("run", run_cfg, "notify_max_actions_per_db", 1)
    _int_at("run", run_cfg, "skip_history_threshold", 1)
    if str(run_cfg.get("json_detail_level", "verbose")).strip().lower() not in ("basic", "verbose"):
        errors.append("run.json_detail_level must be basic or verbose")
    if bool(run_cfg.get("skip_history_enabled", True)) and not str(run_cfg.get("skip_history_path", "") or "").strip():
        errors.append("run.skip_history_path is required when skip_history_enabled is true")

    thresholds_cfg = _require_section("thresholds")
    _float_at("thresholds", thresholds_cfg, "min_table_size_mb", 0)
    _float_at("thresholds", thresholds_cfg, "min_dead_ratio", 0)
    _float_at("thresholds", thresholds_cfg, "max_analyze_age_hours", 0)
    _float_at("thresholds", thresholds_cfg, "max_last_vacuum_age_hours", 0)
    _int_at("thresholds", thresholds_cfg, "freeze_age_threshold", 0)
    vacuum_full_cfg = thresholds_cfg.get("vacuum_full", {}) or {}
    if isinstance(vacuum_full_cfg, dict):
        _hhmm("thresholds.vacuum_full.start", vacuum_full_cfg.get("start", "01:00"))
        _hhmm("thresholds.vacuum_full.end", vacuum_full_cfg.get("end", "05:00"))
        _float_at("thresholds.vacuum_full", vacuum_full_cfg, "min_dead_ratio", 0)
        _float_at("thresholds.vacuum_full", vacuum_full_cfg, "min_table_size_mb", 0)
        _float_at("thresholds.vacuum_full", vacuum_full_cfg, "min_estimated_dead_mb", 0)
        _float_at("thresholds.vacuum_full", vacuum_full_cfg, "min_estimated_dead_ratio", 0)
    else:
        errors.append("thresholds.vacuum_full must be a mapping")

    limits_cfg = _require_section("limits")
    for key in ("max_tables_per_db", "max_actions_global", "sleep_between_tables_sec", "sleep_between_databases_sec"):
        _float_at("limits", limits_cfg, key, 0)
    _int_at("limits", limits_cfg, "parallel_tables_per_db", 1)
    _int_at("limits", limits_cfg, "global_parallel_limit", 1)
    _int_at("limits", limits_cfg, "lock_timeout_ms", 0)
    _int_at("limits", limits_cfg, "per_table_statement_timeout_sec", 1)
    _int_at("limits", limits_cfg, "vacuum_cost_delay_ms", 0)
    _int_at("limits", limits_cfg, "vacuum_cost_limit", 0)

    force_cfg = _require_section("force")
    default_action = str(force_cfg.get("default_action", "ANALYZE")).upper().strip()
    if default_action not in allowed_actions:
        errors.append("force.default_action must be one of {actions}".format(actions=", ".join(sorted(allowed_actions))))
    for idx, item in enumerate(force_cfg.get("tables") or []):
        if isinstance(item, dict):
            action = str(item.get("action", default_action)).upper().strip()
            if action not in allowed_actions:
                errors.append(
                    "force.tables[{idx}].action must be one of {actions}".format(
                        idx=idx, actions=", ".join(sorted(allowed_actions))
                    )
                )

    pol_cfg = _require_section("vacuum_full_policy")
    if str(pol_cfg.get("on_miss", "VACUUM_ANALYZE")).upper().strip() not in ("VACUUM_ANALYZE", "SKIP"):
        errors.append("vacuum_full_policy.on_miss must be VACUUM_ANALYZE or SKIP")

    retry_cfg = _require_section("retry")
    _int_at("retry", retry_cfg, "max_attempts", 1)
    _int_at("retry", retry_cfg, "base_sleep_ms", 0)
    _int_at("retry", retry_cfg, "max_sleep_ms", 0)
    _int_at("retry", retry_cfg, "jitter_ms", 0)
    for idx, ov in enumerate(retry_cfg.get("overrides") or []):
        if not isinstance(ov, dict):
            errors.append("retry.overrides[{idx}] must be a mapping".format(idx=idx))
            continue
        if "start" in ov or "end" in ov:
            if "start" not in ov or "end" not in ov:
                errors.append("retry.overrides[{idx}] must include both start and end".format(idx=idx))
            else:
                _hhmm("retry.overrides[{idx}].start".format(idx=idx), ov.get("start"))
                _hhmm("retry.overrides[{idx}].end".format(idx=idx), ov.get("end"))
        if "max_attempts" in ov:
            _int_at("retry.overrides[{idx}]".format(idx=idx), ov, "max_attempts", 1)

    metrics_cfg = _require_section("metrics")
    if bool(metrics_cfg.get("enabled", False)):
        if not str(metrics_cfg.get("prometheus_textfile", "") or "").strip() and not str(metrics_cfg.get("statsd_host", "") or "").strip():
            errors.append("metrics.prometheus_textfile or metrics.statsd_host is required when metrics.enabled is true")
    _int_at("metrics", metrics_cfg, "statsd_port", 1)
    try:
        if "statsd_port" in metrics_cfg and int(metrics_cfg.get("statsd_port")) > 65535:
            errors.append("metrics.statsd_port must be <= 65535")
    except Exception:
        pass

    if errors:
        raise ValueError("config validation failed:\n- " + "\n- ".join(errors))


def _tz_fixed_offset(timezone_name: str) -> Optional[dt.tzinfo]:
    """
    Minimal fixed-offset timezone support for environments without zoneinfo/pytz.

    For most use cases here, Asia/Seoul (KST, UTC+09:00) is sufficient and has no DST.
    """
    name = (timezone_name or "").strip()
    if name in ("Asia/Seoul", "KST", "ROK", "Asia/Seoul "):
        return dt.timezone(dt.timedelta(hours=9), name="KST")
    if name in ("UTC", "Etc/UTC", "UTC0", "Z"):
        return dt.timezone.utc
    return None


def now_in_tz(timezone_name: str) -> dt.datetime:
    """
    Return current time in the given timezone.

    Priority:
    1) zoneinfo (Python 3.9+)
    2) pytz (optional dependency)
    3) fixed-offset fallback for Asia/Seoul / UTC
    4) naive local time
    """
    # 1) zoneinfo (py3.9+)
    try:
        from zoneinfo import ZoneInfo  # type: ignore

        return dt.datetime.now(tz=ZoneInfo(timezone_name))
    except Exception:
        pass

    # 2) pytz (optional)
    try:
        import pytz  # type: ignore

        tz = pytz.timezone(timezone_name)
        return dt.datetime.now(tz)
    except Exception:
        pass

    # 3) fixed offset for common cases
    tzinfo = _tz_fixed_offset(timezone_name)
    if tzinfo is not None:
        return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).astimezone(tzinfo)

    # 4) fallback (naive)
    return dt.datetime.now()


def bytes_to_mb(size_bytes: int) -> float:
    return size_bytes / (1024 * 1024)


def quote_ident(name: str) -> str:
    """Safely quote an SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def fqtn(schema: str, table: str) -> str:
    """Fully-qualified table name with quoted identifiers."""
    return "{schema}.{table}".format(schema=quote_ident(schema), table=quote_ident(table))


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


def _urllib_post(url: str, data: bytes, headers: Dict[str, str], timeout: int = 5) -> None:
    """POST helper using stdlib urllib (no external deps)."""
    try:
        import urllib.request
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as _:
            pass
    except Exception as e:
        logging.warning("urllib post failed: %s", e)


def slack_notify(webhook_url: str, text: str) -> None:
    """Send a Slack message via incoming webhook."""
    if not webhook_url:
        return

    payload = json.dumps({"text": text}).encode("utf-8")
    if requests is not None:
        try:
            requests.post(webhook_url, json={"text": text}, timeout=5)
            return
        except Exception as e:
            logging.warning("slack_notify (requests) failed, falling back to urllib: %s", e)

    _urllib_post(webhook_url, payload, headers={"Content-Type": "application/json"}, timeout=5)


def telegram_notify(bot_token: str, chat_id: str, text: str) -> None:
    """Send a Telegram message via bot API."""
    if not bot_token or not chat_id:
        return
    url = "https://api.telegram.org/bot{token}/sendMessage".format(token=bot_token)

    if requests is not None:
        try:
            requests.post(url, data={"chat_id": chat_id, "text": text}, timeout=5)
            return
        except Exception as e:
            logging.warning("telegram_notify (requests) failed, falling back to urllib: %s", e)

    try:
        import urllib.parse
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
        _urllib_post(url, data, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=5)
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

    db_cfg["application_name"] = "{base}:{suffix}".format(base=base, suffix=suffix)
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

    # Some old PostgreSQL versions may not support certain settings; ignore if setting fails.
    try:
        pg.execute("set lock_timeout = %s;", ("{ms}ms".format(ms=lock_timeout_ms),))
    except Exception:
        pass

    try:
        pg.execute("set statement_timeout = %s;", ("{ms}ms".format(ms=statement_timeout_ms),))
    except Exception:
        pass

    vacuum_cost_delay_ms = int(limits_cfg.get("vacuum_cost_delay_ms", 0) or 0)
    vacuum_cost_limit = int(limits_cfg.get("vacuum_cost_limit", 0) or 0)

    if vacuum_cost_delay_ms > 0:
        try:
            pg.execute("set vacuum_cost_delay = %s;", ("{ms}ms".format(ms=vacuum_cost_delay_ms),))
        except Exception:
            pass
    if vacuum_cost_limit > 0:
        try:
            pg.execute("set vacuum_cost_limit = %s;", (vacuum_cost_limit,))
        except Exception:
            pass


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
        return "*:{p}".format(p=p)

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

    target_obj = "{schema}.{table}".format(schema=schema, table=table)
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
        found = set([r["datname"] for r in rows])
        missing = [d for d in include_databases if d not in found]
        if missing:
            logging.warning("include_databases contains missing DB(s): %s", missing)
        return [d for d in include_databases if d in found and d not in exclude_databases]

    where = ["1=1"]
    params = []  # type: List[Any]

    if exclude_templates:
        where.append("datistemplate = false")
    if require_allow_conn:
        where.append("datallowconn = true")
    if exclude_databases:
        where.append("datname <> all(%s)")
        params.append(list(exclude_databases))

    sql = """
    select datname
    from pg_database
    where {where}
    order by datname;
    """.format(where=" and ".join(where))
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
    params = []  # type: List[Any]

    for s in exclude_schemas:
        where_parts.append("n.nspname <> %s")
        params.append(s)

    if include_schemas:
        where_parts.append("n.nspname = any(%s)")
        params.append(include_schemas)

    sql = """
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
        age(c.relfrozenxid) as freeze_age,
        coalesce(ps.avg_row_width_bytes, 0) as avg_row_width_bytes
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    left join pg_stat_user_tables st on st.relid = c.oid
    left join (
        select schemaname, tablename, sum(avg_width)::float as avg_row_width_bytes
        from pg_stats
        group by schemaname, tablename
    ) ps on ps.schemaname = n.nspname and ps.tablename = c.relname
    where {where}
    order by pg_total_relation_size(c.oid) desc;
    """.format(where=" and ".join(where_parts))

    rows = pg.fetchall(sql, tuple(params) if params else None)

    candidates = []  # type: List[table_candidate]
    skipped_objects = []  # type: List[Dict[str, Any]]
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
                            "table": "{schema}.{table}".format(schema=schema, table=table),
                            "filter": flt,
                            "stats": {
                                "size_mb": round(bytes_to_mb(int(r["total_size_bytes"] or 0)), 2),
                                "n_live_tup": live,
                                "n_dead_tup": dead,
                                "dead_ratio": round(float(dead_ratio), 8),
                                "avg_row_width_bytes": round(float(r.get("avg_row_width_bytes") or 0), 2),
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

        avg_row_width = float(r.get("avg_row_width_bytes") or 0)
        estimated_dead_bytes = float(dead) * avg_row_width
        total_size_bytes = int(r["total_size_bytes"] or 0)
        estimated_dead_ratio = (estimated_dead_bytes / total_size_bytes) if total_size_bytes > 0 else 0.0

        candidates.append(
            table_candidate(
                schemaname=schema,
                relname=table,
                relid=int(r["relid"]),
                total_size_bytes=total_size_bytes,
                n_live_tup=live,
                n_dead_tup=dead,
                dead_ratio=float(dead_ratio),
                last_vacuum=r["last_vacuum"],
                last_autovacuum=r["last_autovacuum"],
                last_analyze=r["last_analyze"],
                last_autoanalyze=r["last_autoanalyze"],
                freeze_age=freeze_age_int,
                avg_row_width_bytes=avg_row_width,
                estimated_dead_bytes=estimated_dead_bytes,
                estimated_dead_ratio=estimated_dead_ratio,
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

    th_dec = {  # type: Dict[str, Any]
        "evaluated": True,
        "inputs": {
            "size_mb": float(round(size_mb, 4)),
            "dead_ratio": float(round(c.dead_ratio, 8)),
            "avg_row_width_bytes": float(round(c.avg_row_width_bytes, 4)),
            "estimated_dead_mb": float(round(bytes_to_mb(int(c.estimated_dead_bytes)), 4)),
            "estimated_dead_ratio": float(round(c.estimated_dead_ratio, 8)),
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
            "vacuum_full_min_estimated_dead_mb": float((thresholds_cfg.get("vacuum_full", {}) or {}).get("min_estimated_dead_mb", 0) or 0),
            "vacuum_full_min_estimated_dead_ratio": float((thresholds_cfg.get("vacuum_full", {}) or {}).get("min_estimated_dead_ratio", 0) or 0),
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
        return "SKIP", "size_mb<{v}".format(v=min_table_size_mb), th_dec

    vacuum_full_cfg = thresholds_cfg.get("vacuum_full", {}) or {}
    if bool(vacuum_full_cfg.get("enabled", False)):
        full_min_dead_ratio = float(vacuum_full_cfg.get("min_dead_ratio", 0.6))
        full_min_size_mb = float(vacuum_full_cfg.get("min_table_size_mb", 2048))
        full_min_estimated_dead_mb = float(vacuum_full_cfg.get("min_estimated_dead_mb", 0) or 0)
        full_min_estimated_dead_ratio = float(vacuum_full_cfg.get("min_estimated_dead_ratio", 0) or 0)
        start_hhmm = str(vacuum_full_cfg.get("start", "01:00"))
        end_hhmm = str(vacuum_full_cfg.get("end", "05:00"))
        estimated_dead_mb = bytes_to_mb(int(c.estimated_dead_bytes))

        if (
            c.dead_ratio >= full_min_dead_ratio
            and size_mb >= full_min_size_mb
            and estimated_dead_mb >= full_min_estimated_dead_mb
            and c.estimated_dead_ratio >= full_min_estimated_dead_ratio
            and in_time_window(local_now, start_hhmm, end_hhmm)
        ):
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


def _vacuum_full_policy_adjust(
    cfg: Dict[str, Any],
    dbname: str,
    schema: str,
    table: str,
    action: str,
    force_matched: bool,
) -> Tuple[str, Dict[str, Any]]:
    """
    Apply vacuum_full_policy guardrail.

    Returns:
      (possibly_adjusted_action, policy_decision_dict)
    """
    pol = cfg.get("vacuum_full_policy", {}) or {}
    enabled = bool(pol.get("enabled", False))

    dec = {
        "enabled": enabled,
        "applied": False,
        "allow_hit": None,
        "on_miss": None,
        "force_bypass": bool(pol.get("force_bypass", False)),
        "result": None,
        "reason": None,
    }  # type: Dict[str, Any]

    if not enabled:
        return action, dec

    if action != "VACUUM_FULL_ANALYZE":
        return action, dec

    if force_matched and bool(pol.get("force_bypass", False)):
        dec["applied"] = True
        dec["result"] = "allow"
        dec["reason"] = "force_bypass"
        return action, dec

    allow_objects = pol.get("allow_objects") or []
    for pat in allow_objects:
        if match_object_pattern(dbname, schema, table, str(pat)):
            dec["applied"] = True
            dec["allow_hit"] = str(pat)
            dec["result"] = "allow"
            dec["reason"] = "allowlisted"
            return action, dec

    on_miss = str(pol.get("on_miss", "VACUUM_ANALYZE")).upper().strip()
    if on_miss not in ("VACUUM_ANALYZE", "SKIP"):
        on_miss = "VACUUM_ANALYZE"

    dec["applied"] = True
    dec["on_miss"] = on_miss

    if on_miss == "SKIP":
        dec["result"] = "skip"
        dec["reason"] = "not_allowlisted"
        return "SKIP", dec

    dec["result"] = "downgrade"
    dec["reason"] = "not_allowlisted"
    return "VACUUM_ANALYZE", dec


def make_maintenance_sql(schema: str, table: str, action: str) -> str:
    """Build SQL for the maintenance action."""
    target = fqtn(schema, table)

    if action == "ANALYZE":
        return "analyze {t};".format(t=target)
    if action == "VACUUM_ANALYZE":
        return "vacuum (analyze) {t};".format(t=target)
    if action == "VACUUM_FREEZE_ANALYZE":
        return "vacuum (freeze, analyze) {t};".format(t=target)
    if action == "VACUUM_FULL_ANALYZE":
        return "vacuum (full, analyze) {t};".format(t=target)

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


def _safe_lower(s: Any) -> str:
    try:
        return str(s).lower()
    except Exception:
        return ""


def classify_sql_exception(exc: Exception) -> Dict[str, Any]:
    """
    Classify common PostgreSQL errors into retry categories.
    Best-effort based on pgcode + message text.
    """
    pgcode = getattr(exc, "pgcode", None)
    msg = _safe_lower(exc)

    # Important: lock_timeout/statement_timeout often share pgcode=57014 (query_canceled),
    # so detect them by message text first.
    if "canceling statement due to lock timeout" in msg or "lock timeout" in msg:
        cat = "lock_timeout"
    elif "canceling statement due to statement timeout" in msg or "statement timeout" in msg:
        cat = "statement_timeout"
    elif pgcode == "40P01" or "deadlock detected" in msg:
        cat = "deadlock_detected"
    elif pgcode == "40001" or "could not serialize access" in msg or "serialization failure" in msg:
        cat = "serialization_failure"
    elif pgcode == "55P03" or "lock not available" in msg or "could not obtain lock" in msg:
        cat = "lock_not_available"
    elif pgcode == "57014" or "canceling statement due to user request" in msg or "query canceled" in msg:
        cat = "query_canceled"
    else:
        cat = "other"

    return {
        "category": cat,
        "pgcode": pgcode,
        "message": str(exc),
        "status": "FAIL" if cat == "other" else "SKIP",
        "retryable": cat != "other" and cat != "query_canceled",
    }


def _retry_policy_for_task(cfg: Dict[str, Any], dbname: str, action: str, local_now: dt.datetime) -> Dict[str, Any]:
    """
    Compute effective retry policy for a task, applying overrides in order.
    """
    base = cfg.get("retry", {}) or {}

    pol = {
        "enabled": bool(base.get("enabled", False)),
        "max_attempts": int(base.get("max_attempts", 1) or 1),
        "base_sleep_ms": int(base.get("base_sleep_ms", 200) or 200),
        "max_sleep_ms": int(base.get("max_sleep_ms", 5000) or 5000),
        "jitter_ms": int(base.get("jitter_ms", 0) or 0),
        "retryable_categories": list(base.get("retryable_categories") or []),
    }  # type: Dict[str, Any]

    overrides = base.get("overrides") or []
    for ov in overrides:
        if not isinstance(ov, dict):
            continue

        db_pat = str(ov.get("db_pattern", "*") or "*")
        if not fnmatch.fnmatchcase(dbname, db_pat):
            continue

        act_pat = str(ov.get("action_pattern", "*") or "*")
        if act_pat and not fnmatch.fnmatchcase(action, act_pat):
            continue

        start = ov.get("start")
        end = ov.get("end")
        if start is not None and end is not None:
            try:
                if not in_time_window(local_now, str(start), str(end)):
                    continue
            except Exception:
                continue

        # Apply override fields
        if "enabled" in ov:
            pol["enabled"] = bool(ov.get("enabled", False))
        if "max_attempts" in ov:
            pol["max_attempts"] = int(ov.get("max_attempts", pol["max_attempts"]) or pol["max_attempts"])
        if "base_sleep_ms" in ov:
            pol["base_sleep_ms"] = int(ov.get("base_sleep_ms", pol["base_sleep_ms"]) or pol["base_sleep_ms"])
        if "max_sleep_ms" in ov:
            pol["max_sleep_ms"] = int(ov.get("max_sleep_ms", pol["max_sleep_ms"]) or pol["max_sleep_ms"])
        if "jitter_ms" in ov:
            pol["jitter_ms"] = int(ov.get("jitter_ms", pol["jitter_ms"]) or pol["jitter_ms"])
        if "retryable_categories" in ov and isinstance(ov.get("retryable_categories"), list):
            pol["retryable_categories"] = list(ov.get("retryable_categories") or pol["retryable_categories"])

    # Normalize
    if not pol.get("enabled", False):
        pol["enabled"] = False
        pol["max_attempts"] = 1

    try:
        pol["max_attempts"] = max(1, int(pol.get("max_attempts", 1)))
    except Exception:
        pol["max_attempts"] = 1

    try:
        pol["base_sleep_ms"] = max(0, int(pol.get("base_sleep_ms", 0)))
    except Exception:
        pol["base_sleep_ms"] = 0

    try:
        pol["max_sleep_ms"] = max(0, int(pol.get("max_sleep_ms", 0)))
    except Exception:
        pol["max_sleep_ms"] = 0

    try:
        pol["jitter_ms"] = max(0, int(pol.get("jitter_ms", 0)))
    except Exception:
        pol["jitter_ms"] = 0

    if pol["max_sleep_ms"] < pol["base_sleep_ms"]:
        pol["max_sleep_ms"] = pol["base_sleep_ms"]

    return pol


def _precheck_progress_vacuum_rows(pg: pg_client, relid: int) -> List[Dict[str, Any]]:
    """
    Best-effort fetch rows from pg_stat_progress_vacuum joined with pg_stat_activity.
    If view is unavailable or permissions are insufficient, returns [].
    """
    try:
        sql = """
        select a.pid, a.query
        from pg_stat_progress_vacuum p
        join pg_stat_activity a on a.pid = p.pid
        where p.relid = %s
          and a.pid <> pg_backend_pid();
        """
        return pg.fetchall(sql, (relid,))
    except Exception:
        return []


def _precheck_activity_match(pg: pg_client, like_prefix: str, schema: str, table: str) -> bool:
    """
    Best-effort fallback using pg_stat_activity query text matching.
    """
    try:
        sql = """
        select 1 as ok
        from pg_stat_activity
        where pid <> pg_backend_pid()
          and state <> 'idle'
          and query ilike %s
          and query ilike %s
          and query ilike %s
        limit 1;
        """
        row = pg.fetchone(sql, (like_prefix, "%{s}%".format(s=schema), "%{t}%".format(t=table)))
        return bool(row)
    except Exception:
        return False


def _precheck_relation_locked(pg: pg_client, relid: int) -> bool:
    """
    Return True if there is any granted lock on the relation by other backends.
    (Used as a conservative guard for VACUUM FULL.)
    """
    try:
        sql = """
        select 1 as ok
        from pg_locks l
        where l.relation = %s
          and l.pid <> pg_backend_pid()
          and l.granted = true
        limit 1;
        """
        row = pg.fetchone(sql, (relid,))
        return bool(row)
    except Exception:
        return False


def precheck_should_skip(
    pg: pg_client,
    cfg: Dict[str, Any],
    task: action_task,
    action: str,
) -> Dict[str, Any]:
    """
    Best-effort precheck before executing a maintenance action.

    Returns dict:
      { "enabled": bool, "skip": bool, "skip_reason": str, "details": {...} }
    """
    pre = cfg.get("precheck", {}) or {}
    enabled = bool(pre.get("enabled", False))

    out = {
        "enabled": enabled,
        "skip": False,
        "skip_reason": "",
        "details": {},
    }  # type: Dict[str, Any]

    if not enabled:
        return out

    c = task.candidate
    schema = c.schemaname
    table = c.relname
    relid = int(c.relid)

    skip_if_autovacuum_running = bool(pre.get("skip_if_autovacuum_running", False))
    skip_if_vacuum_running = bool(pre.get("skip_if_vacuum_running", False))
    skip_if_analyze_running = bool(pre.get("skip_if_analyze_running", False))
    skip_vacuum_full_if_relation_locked = bool(pre.get("skip_vacuum_full_if_relation_locked", False))

    # 1) VACUUM FULL conservative lock check
    if action == "VACUUM_FULL_ANALYZE" and skip_vacuum_full_if_relation_locked:
        locked = _precheck_relation_locked(pg, relid)
        out["details"]["vacuum_full_relation_locked"] = locked
        if locked:
            out["skip"] = True
            out["skip_reason"] = "precheck:relation_locked_for_vacuum_full"
            return out

    # 2) Prefer pg_stat_progress_vacuum (covers vacuum/autovacuum in progress)
    rows = _precheck_progress_vacuum_rows(pg, relid)
    if rows:
        is_autovac = False
        is_vac = False
        for r in rows:
            q = _safe_lower(r.get("query", ""))
            if q.startswith("autovacuum:"):
                is_autovac = True
            else:
                is_vac = True

        out["details"]["progress_vacuum_rows"] = len(rows)
        out["details"]["autovacuum_running"] = is_autovac
        out["details"]["vacuum_running"] = is_vac

        if skip_if_autovacuum_running and is_autovac:
            out["skip"] = True
            out["skip_reason"] = "precheck:autovacuum_running"
            return out

        if skip_if_vacuum_running and is_vac:
            out["skip"] = True
            out["skip_reason"] = "precheck:vacuum_running"
            return out

    # 3) Fallback: pg_stat_activity query-text matching (best-effort)
    if skip_if_autovacuum_running:
        hit = _precheck_activity_match(pg, "autovacuum:%", schema, table)
        out["details"]["autovacuum_activity_match"] = hit
        if hit:
            out["skip"] = True
            out["skip_reason"] = "precheck:autovacuum_running(activity)"
            return out

    if skip_if_vacuum_running:
        hit = _precheck_activity_match(pg, "vacuum%", schema, table)
        out["details"]["vacuum_activity_match"] = hit
        if hit:
            out["skip"] = True
            out["skip_reason"] = "precheck:vacuum_running(activity)"
            return out

    if skip_if_analyze_running:
        hit = _precheck_activity_match(pg, "analyze%", schema, table)
        out["details"]["analyze_activity_match"] = hit
        if hit:
            out["skip"] = True
            out["skip_reason"] = "precheck:analyze_running(activity)"
            return out

    return out


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

    entry = {  # type: Dict[str, Any]
        "db": task.dbname,
        "table": "{schema}.{table}".format(schema=c.schemaname, table=c.relname),
        "action": task.action,
        "reason": task.reason,
        "ok": False,
        "status": "FAIL",
        "skipped": False,
        "skip_reason": "",
        "skipped_by_stop": False,
    }

    if json_detail_level == "verbose":
        entry["decision"] = task.decision

    if not sql:
        entry["ok"] = True
        entry["status"] = "OK"
        entry["reason"] = "no_sql"
        return entry

    if immediate_stop_event.is_set():
        entry["skipped"] = True
        entry["skipped_by_stop"] = True
        entry["skip_reason"] = "immediate_stop"
        entry["status"] = "SKIP"
        entry["reason"] = "immediate_stop"
        return entry

    if graceful_stop_event.is_set():
        entry["skipped"] = True
        entry["skipped_by_stop"] = True
        entry["skip_reason"] = "graceful_stop"
        entry["status"] = "SKIP"
        entry["reason"] = "graceful_stop"
        return entry

    # Acquire global semaphore with short timeouts so stop signals can break quickly.
    acquired = False
    while not acquired:
        if immediate_stop_event.is_set():
            entry["skipped"] = True
            entry["skipped_by_stop"] = True
            entry["skip_reason"] = "immediate_stop"
            entry["status"] = "SKIP"
            entry["reason"] = "immediate_stop"
            return entry
        if graceful_stop_event.is_set():
            entry["skipped"] = True
            entry["skipped_by_stop"] = True
            entry["skip_reason"] = "graceful_stop"
            entry["status"] = "SKIP"
            entry["reason"] = "graceful_stop"
            return entry
        acquired = global_sem.acquire(timeout=0.5)

    pid = None  # type: Optional[int]
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
        "attempts": [],
    }

    try:
        if immediate_stop_event.is_set():
            entry["skipped"] = True
            entry["skipped_by_stop"] = True
            entry["skip_reason"] = "immediate_stop"
            entry["status"] = "SKIP"
            entry["reason"] = "immediate_stop"
            return entry

        db_cfg = cfg_for_db(base_cfg, task.dbname)
        db_cfg = cfg_with_application_suffix(db_cfg, "worker")

        # For override time windows, use run.timezone
        timezone_name = str((base_cfg.get("run", {}) or {}).get("timezone", "Asia/Seoul"))
        local_now = now_in_tz(timezone_name)

        retry_pol = _retry_policy_for_task(base_cfg, task.dbname, task.action, local_now)

        with pg_client(db_cfg, context="worker:{db}".format(db=task.dbname)) as pg:
            apply_session_settings(pg, base_cfg)
            pid = register_active_conn(pg)
            entry["execution"]["backend_pid"] = pid

            # Precheck
            precheck_res = precheck_should_skip(pg, base_cfg, task, task.action)
            if json_detail_level == "verbose":
                entry["precheck"] = precheck_res

            if precheck_res.get("skip", False):
                entry["skipped"] = True
                entry["skip_reason"] = str(precheck_res.get("skip_reason", "precheck_skip"))
                entry["status"] = "SKIP"
                entry["reason"] = "precheck:{r}".format(r=entry["skip_reason"])
                return entry

            if dry_run:
                logging.info("[DRY-RUN] db=%s %s", task.dbname, sql)
                entry["ok"] = True
                entry["status"] = "OK"
                return entry

            # Retry loop
            max_attempts = int(retry_pol.get("max_attempts", 1) or 1)
            retryable = set([str(x) for x in (retry_pol.get("retryable_categories") or [])])

            for attempt in range(1, max_attempts + 1):
                if immediate_stop_event.is_set():
                    entry["skipped"] = True
                    entry["skipped_by_stop"] = True
                    entry["skip_reason"] = "immediate_stop"
                    entry["status"] = "SKIP"
                    entry["reason"] = "immediate_stop"
                    return entry
                if graceful_stop_event.is_set():
                    entry["skipped"] = True
                    entry["skipped_by_stop"] = True
                    entry["skip_reason"] = "graceful_stop"
                    entry["status"] = "SKIP"
                    entry["reason"] = "graceful_stop"
                    return entry

                try:
                    logging.info("exec db=%s %s", task.dbname, sql)
                    pg.execute(sql)

                    entry["execution"]["attempts"].append(
                        {"attempt": attempt, "ok": True, "category": None, "sleep_ms": 0, "error": None}
                    )
                    entry["ok"] = True
                    entry["status"] = "OK"
                    return entry

                except Exception as e:
                    info = classify_sql_exception(e)
                    cat = str(info.get("category", "other"))
                    err_txt = str(e)

                    sleep_ms = 0
                    will_retry = False

                    if attempt < max_attempts and (cat in retryable):
                        # backoff: base * 2^(attempt-1)
                        base_sleep = int(retry_pol.get("base_sleep_ms", 0) or 0)
                        max_sleep = int(retry_pol.get("max_sleep_ms", base_sleep) or base_sleep)
                        jitter = int(retry_pol.get("jitter_ms", 0) or 0)

                        raw = base_sleep * (2 ** (attempt - 1))
                        sleep_ms = min(max_sleep, raw)

                        if jitter > 0:
                            sleep_ms = sleep_ms + random.randint(-jitter, jitter)
                            if sleep_ms < 0:
                                sleep_ms = 0

                        will_retry = True

                    entry["execution"]["attempts"].append(
                        {
                            "attempt": attempt,
                            "ok": False,
                            "category": cat,
                            "sleep_ms": sleep_ms,
                            "error": err_txt,
                            "pgcode": info.get("pgcode"),
                        }
                    )

                    if not will_retry:
                        entry["execution"]["error"] = err_txt
                        if str(info.get("status")) == "SKIP":
                            entry["skipped"] = True
                            entry["skip_reason"] = cat
                            entry["status"] = "SKIP"
                        else:
                            entry["ok"] = False
                            entry["status"] = "FAIL"
                        return entry

                    logging.warning(
                        "retryable_error db=%s table=%s.%s action=%s attempt=%d/%d category=%s sleep_ms=%d err=%s",
                        task.dbname,
                        c.schemaname,
                        c.relname,
                        task.action,
                        attempt,
                        max_attempts,
                        cat,
                        sleep_ms,
                        err_txt,
                    )

                    time.sleep(sleep_ms / 1000.0)

            # Should not reach here
            entry["execution"]["error"] = entry["execution"].get("error") or "retry_exhausted"
            entry["ok"] = False
            entry["status"] = "FAIL"
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
        entry["status"] = "FAIL"
        return entry

    finally:
        entry["execution"]["ended_at"] = dt.datetime.now().isoformat()
        entry["execution"]["elapsed_ms"] = int((time.time() - t0) * 1000)
        unregister_active_conn(pid)
        if acquired:
            global_sem.release()


def resolve_run_mode(args: argparse.Namespace, cfg: Dict[str, Any]) -> bool:
    """Return True for dry-run, False for apply."""
    if args.apply:
        return False
    if args.dry_run:
        return True
    return bool((cfg.get("run", {}) or {}).get("dry_run_default", True))


def resolve_exit_code(run_summary: Dict[str, Any]) -> int:
    """Return process exit code for scheduler/monitoring integration."""
    if bool(run_summary.get("aborted", False)):
        return 5

    g = run_summary.get("global", {}) or {}
    if bool(run_summary.get("json_save_failed", False)) and bool(g.get("json_fail_on_error", True)):
        return 7

    if int(g.get("executed_fail", 0) or 0) > 0:
        return 4

    sh = run_summary.get("skip_history", {}) or {}
    if bool(sh.get("fail_on_threshold", True)) and int(sh.get("alert_count", 0) or 0) > 0:
        return 8

    for db in run_summary.get("db_results", []) or []:
        reason = str(db.get("skipped_reason", "") or "")
        if reason and reason != "standby":
            return 6

    return 0


def install_signal_handlers() -> None:
    """
    Install signal handlers:
    - SIGINT: 1st => graceful stop; 2nd => immediate stop + cancel
    - SIGTERM: immediate stop + cancel
    """
    def on_sigint(signum, frame):
        # pylint: disable=global-statement
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
    if not os.path.isdir(path):
        os.makedirs(path)


def default_json_out_path(run_cfg: Dict[str, Any], local_now: dt.datetime) -> str:
    """Build default JSON output path based on config prefix and current time."""
    out_dir = str(run_cfg.get("json_out_dir", ".") or ".")
    prefix = str(run_cfg.get("json_out_prefix", "run") or "run")
    ensure_dir(out_dir)
    ts = local_now.strftime("%Y%m%d_%H%M%S")
    return os.path.join(out_dir, "{prefix}_{ts}.json".format(prefix=prefix, ts=ts))


def normalize_action_status(entry: Dict[str, Any]) -> str:
    """Normalize action result into OK/SKIP/FAIL."""
    st = str(entry.get("status") or "").upper().strip()
    if st in allowed_statuses:
        return st
    if entry.get("ok"):
        return "OK"
    if entry.get("skipped") or entry.get("skipped_by_stop"):
        return "SKIP"
    return "FAIL"


def iter_action_results(run_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten per-database action results."""
    out = []  # type: List[Dict[str, Any]]
    for db in run_summary.get("db_results", []) or []:
        for action in db.get("actions", []) or []:
            if isinstance(action, dict):
                out.append(action)
    return out


def get_skip_history_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return repeated-SKIP history settings."""
    run_cfg = cfg.get("run", {}) or {}
    threshold = int(run_cfg.get("skip_history_threshold", 3) or 3)
    if threshold < 1:
        threshold = 3
    return {
        "enabled": bool(run_cfg.get("skip_history_enabled", True)),
        "path": str(run_cfg.get("skip_history_path", "./runs/skip_history.json") or "./runs/skip_history.json"),
        "threshold": threshold,
        "fail_on_threshold": bool(run_cfg.get("skip_history_fail_on_threshold", True)),
        "reset_on_ok": bool(run_cfg.get("skip_history_reset_on_ok", True)),
    }


def update_skip_history(cfg: Dict[str, Any], run_summary: Dict[str, Any], local_now: dt.datetime) -> None:
    """Track repeated table-level SKIP results across runs."""
    scfg = get_skip_history_cfg(cfg)
    summary = {
        "enabled": bool(scfg.get("enabled", False)),
        "path": scfg.get("path"),
        "threshold": int(scfg.get("threshold", 3)),
        "fail_on_threshold": bool(scfg.get("fail_on_threshold", True)),
        "alerts": [],
        "updated": False,
        "error": None,
    }
    run_summary["skip_history"] = summary

    if not summary["enabled"]:
        return

    path = str(scfg.get("path") or "")
    if not path:
        summary["error"] = "skip_history_path_empty"
        return

    history = {}  # type: Dict[str, Any]
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    history = loaded
    except Exception as e:
        logging.warning("skip_history_load_failed path=%s error=%s", path, str(e))
        summary["error"] = "load_failed: {e}".format(e=e)
        history = {}

    now_iso = local_now.isoformat()
    threshold = int(scfg.get("threshold", 3))
    reset_on_ok = bool(scfg.get("reset_on_ok", True))
    alerts = []  # type: List[Dict[str, Any]]

    for action in iter_action_results(run_summary):
        db = str(action.get("db") or "")
        table = str(action.get("table") or "")
        act = str(action.get("action") or "")
        status = normalize_action_status(action)

        if status == "OK" and reset_on_ok:
            prefix = "{db}|{table}|{action}|".format(db=db, table=table, action=act)
            for key in list(history.keys()):
                if key.startswith(prefix):
                    history.pop(key, None)
            continue

        if status != "SKIP":
            continue

        reason = str(action.get("skip_reason") or action.get("reason") or "skipped")
        key = "{db}|{table}|{action}|{reason}".format(db=db, table=table, action=act, reason=reason)
        rec = history.get(key)
        if not isinstance(rec, dict):
            rec = {"count": 0, "db": db, "table": table, "action": act, "reason": reason, "first_seen": now_iso}

        rec["count"] = int(rec.get("count", 0) or 0) + 1
        rec["db"] = db
        rec["table"] = table
        rec["action"] = act
        rec["reason"] = reason
        rec["last_seen"] = now_iso
        history[key] = rec

        if int(rec["count"]) >= threshold:
            alerts.append(dict(rec))

    summary["alerts"] = alerts
    summary["alert_count"] = len(alerts)

    try:
        ensure_dir(os.path.dirname(path))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2, sort_keys=True)
        summary["updated"] = True
    except Exception as e:
        logging.warning("skip_history_save_failed path=%s error=%s", path, str(e))
        summary["error"] = "save_failed: {e}".format(e=e)


def get_metrics_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return metrics export settings."""
    mcfg = cfg.get("metrics", {}) or {}
    return {
        "enabled": bool(mcfg.get("enabled", False)),
        "prometheus_textfile": str(mcfg.get("prometheus_textfile", "") or ""),
        "statsd_host": str(mcfg.get("statsd_host", "") or ""),
        "statsd_port": int(mcfg.get("statsd_port", 8125) or 8125),
        "statsd_prefix": str(mcfg.get("statsd_prefix", "pg_vacman") or "pg_vacman"),
    }


def build_metric_values(run_summary: Dict[str, Any], exit_code: int) -> Dict[str, int]:
    """Build integer metric values from run summary."""
    g = run_summary.get("global", {}) or {}
    sh = run_summary.get("skip_history", {}) or {}
    return {
        "planned_actions": int(g.get("planned_actions", 0) or 0),
        "executed_ok": int(g.get("executed_ok", 0) or 0),
        "executed_skip": int(g.get("executed_skip", 0) or 0),
        "executed_fail": int(g.get("executed_fail", 0) or 0),
        "skipped_dbs": int(g.get("skipped_dbs", 0) or 0),
        "aborted": 1 if bool(run_summary.get("aborted", False)) else 0,
        "json_save_failed": 1 if bool(run_summary.get("json_save_failed", False)) else 0,
        "skip_history_alerts": int(sh.get("alert_count", 0) or 0),
        "exit_code": int(exit_code),
    }


def write_metrics(cfg: Dict[str, Any], run_summary: Dict[str, Any], exit_code: int) -> None:
    """Best-effort metrics export to Prometheus textfile and/or StatsD."""
    mcfg = get_metrics_cfg(cfg)
    if not mcfg.get("enabled", False):
        return

    values = build_metric_values(run_summary, exit_code)
    textfile = str(mcfg.get("prometheus_textfile") or "")
    if textfile:
        try:
            ensure_dir(os.path.dirname(textfile))
            lines = [
                "# HELP pg_vacman_run_metric pg_vacman run-level metric",
                "# TYPE pg_vacman_run_metric gauge",
            ]
            for name, value in sorted(values.items()):
                lines.append('pg_vacman_run_metric{{metric="{name}"}} {value}'.format(name=name, value=int(value)))
            with open(textfile, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            logging.info("metrics_prometheus_textfile_saved path=%s", textfile)
        except Exception as e:
            logging.warning("metrics_prometheus_textfile_failed path=%s error=%s", textfile, str(e))

    host = str(mcfg.get("statsd_host") or "")
    if host:
        port = int(mcfg.get("statsd_port", 8125) or 8125)
        prefix = str(mcfg.get("statsd_prefix") or "pg_vacman")
        try:
            payload = "\n".join(
                ["{prefix}.{name}:{value}|g".format(prefix=prefix, name=name, value=int(value)) for name, value in sorted(values.items())]
            )
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.sendto(payload.encode("utf-8"), (host, port))
            finally:
                sock.close()
            logging.info("metrics_statsd_sent host=%s port=%d", host, port)
        except Exception as e:
            logging.warning("metrics_statsd_failed host=%s port=%s error=%s", host, port, str(e))


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
        return "{v}ms".format(v=v)
    return "{s}s".format(s=round(v / 1000.0, 2))


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
    - Per DB: planned/ok/fail/skip or skip reason
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
    skip = g.get("executed_skip", 0)
    skipped_dbs = g.get("skipped_dbs", 0)

    aborted = bool(run_summary.get("aborted", False))
    abort_mode = run_summary.get("abort_mode", "None")

    parallel_db = g.get("parallel_tables_per_db", "")
    parallel_global = g.get("global_parallel_limit", "")
    detail = run_summary.get("detail_level", "")

    lines = []  # type: List[str]
    lines.append("[pg_vacman] {mode} {ts} ({tz})".format(mode=mode, ts=ts, tz=timezone_name))
    lines.append(
        "planned={planned} ok={ok} fail={fail} skip={skip} skipped_dbs={skipped} aborted={aborted}({abort_mode})".format(
            planned=planned, ok=ok, fail=fail, skip=skip, skipped=skipped_dbs, aborted=aborted, abort_mode=abort_mode
        )
    )
    lines.append(
        "detail={detail} parallel(db={pdb}/global={pgl})".format(detail=detail, pdb=parallel_db, pgl=parallel_global)
    )

    db_results = run_summary.get("db_results", []) or []
    for db in db_results:
        dbname = db.get("db", "")
        db_planned = db.get("planned", 0)
        db_ok = db.get("ok", 0)
        db_fail = db.get("fail", 0)
        db_skip = db.get("skip", 0)
        skipped_reason = db.get("skipped_reason", "")

        lines.append("")
        if skipped_reason:
            lines.append("DB: {db} skipped_reason={r}".format(db=dbname, r=skipped_reason))
            continue

        lines.append(
            "DB: {db} planned={p} ok={ok} fail={f} skip={s}".format(
                db=dbname, p=db_planned, ok=db_ok, f=db_fail, s=db_skip
            )
        )

        actions = db.get("actions", []) or []

        # Sort: FAIL first, then SKIP, then OK
        def _sort_key(a: Dict[str, Any]) -> Tuple[int, str]:
            st = str(a.get("status") or "").upper().strip()
            if st not in allowed_statuses:
                if a.get("ok"):
                    st = "OK"
                elif a.get("skipped") or a.get("skipped_by_stop"):
                    st = "SKIP"
                else:
                    st = "FAIL"
            grp = {"FAIL": 0, "SKIP": 1, "OK": 2}.get(st, 9)
            return (grp, str(a.get("table", "")))

        actions_sorted = sorted(actions, key=_sort_key)

        lines.append("status | action | table | elapsed | message" + (" | sql" if include_sql else ""))

        shown = 0
        for a in actions_sorted:
            if shown >= max_actions_per_db:
                break

            status = str(a.get("status") or "").upper().strip()
            if status not in allowed_statuses:
                if a.get("ok"):
                    status = "OK"
                elif a.get("skipped") or a.get("skipped_by_stop"):
                    status = "SKIP"
                else:
                    status = "FAIL"

            action = a.get("action", "")
            table = a.get("table", "")
            ex = a.get("execution", {}) or {}
            elapsed = _fmt_elapsed_ms(ex.get("elapsed_ms"))

            if status == "OK":
                msg = _one_line(a.get("reason", ""), 180)
            elif status == "SKIP":
                msg = _one_line(a.get("skip_reason") or a.get("reason", "skipped"), 180)
            else:
                err = _one_line(ex.get("error"), 180)
                msg = err if err else _one_line(a.get("reason", ""), 180)

            row = "{status} | {action} | {table} | {elapsed} | {msg}".format(
                status=status, action=action, table=table, elapsed=elapsed, msg=msg
            ).rstrip()

            if include_sql:
                sql = _one_line(ex.get("sql", ""), 180)
                row = "{row} | {sql}".format(row=row, sql=sql).rstrip()

            lines.append(row)
            shown += 1

        remaining = len(actions_sorted) - shown
        if remaining > 0:
            lines.append("... and {n} more actions in {db}".format(n=remaining, db=dbname))

    if json_path:
        lines.append("")
        lines.append("run.json: {p}".format(p=json_path))

    text = "\n".join(lines).strip()
    return "```\n{t}\n```".format(t=text)


def main() -> int:
    ap = argparse.ArgumentParser(description="PostgreSQL maintenance manager (multi-db loop)")
    ap.add_argument("--config", required=True, help="config.yaml path")
    ap.add_argument("--dry-run", action="store_true", help="do not execute, only print plan")
    ap.add_argument("--apply", action="store_true", help="execute actions")
    ap.add_argument("--json-out", default="", help="write run result json to path (if empty, auto path is used)")
    args = ap.parse_args()

    try:
        cfg = load_config(args.config)
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1

    run_cfg = cfg.get("run", {}) or {}

    setup_logging(str(run_cfg.get("log_level", "info")))
    try:
        validate_config(cfg)
    except Exception as e:
        logging.error("%s", str(e))
        return 1

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
    json_fail_on_error = bool(run_cfg.get("json_fail_on_error", True))

    notify_max_actions_per_db = int(run_cfg.get("notify_max_actions_per_db", 30) or 30)
    if notify_max_actions_per_db < 1:
        notify_max_actions_per_db = 30

    notify_include_sql = bool(run_cfg.get("notify_include_sql", False))

    global_sem = threading.Semaphore(global_parallel_limit)

    filters_snapshot = build_filters_snapshot(cfg)

    run_summary = {  # type: Dict[str, Any]
        "ts": local_now.isoformat(),
        "dry_run": dry_run,
        "aborted": False,
        "abort_mode": "None",
        "detail_level": json_detail_level,
        "json_path": "",
        "json_save_failed": False,
        "json_save_error": None,
        "filters_snapshot": filters_snapshot if json_detail_level == "verbose" else {},
        "global": {
            "planned_actions": 0,
            "executed_ok": 0,
            "executed_fail": 0,
            "executed_skip": 0,
            "skipped_dbs": 0,
            "parallel_tables_per_db": parallel_tables_per_db,
            "global_parallel_limit": global_parallel_limit,
            "max_tables_per_db": max_tables_per_db,
            "max_actions_global": max_actions_global,
            "json_max_skips_per_db": json_max_skips_per_db,
            "json_fail_on_error": json_fail_on_error,
        },
        "db_results": [],
    }

    cfg_control = cfg_with_application_suffix(cfg, "control")
    pg_lock_holder = None  # type: Optional[pg_client]
    pg_lock = None  # type: Optional[pg_client]

    # Keep the session-level advisory lock connection open for the whole run.
    try:
        pg_lock_holder = pg_client(cfg_control, context="control_lock")
        pg_lock = pg_lock_holder.__enter__()
        if not try_advisory_lock(pg_lock, advisory_lock_key):
            msg = "[pg_vacman] already running (advisory_lock_key={k})".format(k=advisory_lock_key)
            logging.warning(msg)
            slack_notify(notify_cfg.get("slack_webhook_url", ""), msg)
            telegram_notify(notify_cfg.get("telegram_bot_token", ""), notify_cfg.get("telegram_chat_id", ""), msg)
            pg_lock_holder.__exit__(None, None, None)
            pg_lock_holder = None
            return 2

        with pg_client(cfg_control, context="control") as pg_ctrl:
            apply_session_settings(pg_ctrl, cfg)
            try:
                db_list = list_target_databases(pg_ctrl, cfg)
                if max_databases_per_run > 0:
                    db_list = db_list[:max_databases_per_run]
                logging.info("target_databases=%d %s", len(db_list), db_list)
            except Exception as e:
                logging.exception("failed to list databases: %s", e)
                release_advisory_lock(pg_lock, advisory_lock_key)
                pg_lock_holder.__exit__(None, None, None)
                pg_lock_holder = None
                return 1
    except Exception:
        try:
            if pg_lock_holder is not None:
                pg_lock_holder.__exit__(*sys.exc_info())
        except Exception:
            pass
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

            db_report = {  # type: Dict[str, Any]
                "db": dbname,
                "planned": 0,
                "ok": 0,
                "fail": 0,
                "skip": 0,
                "actions": [],
            }

            if json_detail_level == "verbose":
                db_report["filters_snapshot"] = filters_snapshot
                db_report["skipped_objects"] = []
                db_report["skipped_objects_overflow"] = 0

            candidates = []  # type: List[table_candidate]

            try:
                db_cfg_ctrl = cfg_for_db(cfg_control, dbname)
                with pg_client(db_cfg_ctrl, context="candidate:{db}".format(db=dbname)) as pg_db_ctrl:
                    apply_session_settings(pg_db_ctrl, cfg)
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
                db_report["skipped_reason"] = "candidate_fetch_error: {e}".format(e=e)
                run_summary["global"]["skipped_dbs"] += 1
                run_summary["db_results"].append(db_report)
                if sleep_between_databases_sec > 0:
                    time.sleep(sleep_between_databases_sec)
                continue

            plans = []  # type: List[action_task]

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

                # Apply VACUUM FULL policy guardrail (allowlist / downgrade / skip)
                action2, vf_pol_dec = _vacuum_full_policy_adjust(
                    cfg=cfg,
                    dbname=dbname,
                    schema=schema,
                    table=table,
                    action=action,
                    force_matched=bool(force_dec.get("matched", False)),
                )
                if vf_pol_dec.get("applied"):
                    # annotate
                    try:
                        th_dec = th_dec or {}
                    except Exception:
                        th_dec = {}
                final_action = action2

                decision = {
                    "filter": flt,
                    "force": force_dec,
                    "thresholds": th_dec,
                    "vacuum_full_policy": vf_pol_dec,
                    "final_action": {
                        "action": final_action,
                        "reason": reason,
                        "source": source,
                    },
                }

                if final_action != "SKIP":
                    plans.append(
                        action_task(
                            dbname=dbname,
                            candidate=c,
                            action=final_action,
                            reason=reason,
                            decision=decision,
                        )
                    )

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

            def _count_result(res: Dict[str, Any]) -> None:
                st = str(res.get("status") or "").upper().strip()
                if st not in allowed_statuses:
                    if res.get("ok"):
                        st = "OK"
                    elif res.get("skipped") or res.get("skipped_by_stop"):
                        st = "SKIP"
                    else:
                        st = "FAIL"
                if st == "OK":
                    db_report["ok"] += 1
                    run_summary["global"]["executed_ok"] += 1
                elif st == "SKIP":
                    db_report["skip"] += 1
                    run_summary["global"]["executed_skip"] += 1
                else:
                    db_report["fail"] += 1
                    run_summary["global"]["executed_fail"] += 1

            if parallel_tables_per_db == 1:
                for t in plans:
                    if immediate_stop_event.is_set():
                        run_summary["aborted"] = True
                        run_summary["abort_mode"] = "immediate"
                        break

                    res = vacuum_worker(cfg, t, dry_run, global_sem, json_detail_level)
                    db_report["actions"].append(res)
                    _count_result(res)

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
                        _count_result(res)

                        if sleep_between_tables_sec > 0:
                            time.sleep(sleep_between_tables_sec)

            run_summary["db_results"].append(db_report)
            logging.info(
                "db_end=%s planned=%d ok=%d fail=%d skip=%d",
                dbname,
                db_report["planned"],
                db_report["ok"],
                db_report["fail"],
                db_report["skip"],
            )

            if sleep_between_databases_sec > 0:
                time.sleep(sleep_between_databases_sec)

        update_skip_history(cfg, run_summary, local_now)
        run_summary["exit_code"] = resolve_exit_code(run_summary)

        # JSON output policy:
        # - If --json-out is provided: write there
        # - Else: if json_auto_save is true, write to default path
        json_path = args.json_out.strip()
        if not json_path and json_auto_save:
            json_path = default_json_out_path(run_cfg, local_now)
        run_summary["json_path"] = json_path

        if json_path:
            try:
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(run_summary, f, ensure_ascii=False, indent=2)
                logging.info("json_saved path=%s", json_path)
            except Exception as e:
                run_summary["json_save_failed"] = True
                run_summary["json_save_error"] = str(e)
                logging.error("json_save_failed path=%s error=%s", json_path, str(e))

        exit_code = resolve_exit_code(run_summary)
        run_summary["exit_code"] = exit_code
        write_metrics(cfg, run_summary, exit_code)

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

        if exit_code != 0:
            logging.warning("run_exit_code=%d", exit_code)
        return exit_code

    finally:
        try:
            if pg_lock is not None:
                release_advisory_lock(pg_lock, advisory_lock_key)
        except Exception:
            pass
        try:
            if pg_lock_holder is not None:
                pg_lock_holder.__exit__(None, None, None)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
