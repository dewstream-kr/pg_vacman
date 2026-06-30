import datetime as dt
import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path


def _install_import_stubs():
    requests_mod = types.ModuleType("requests")
    requests_mod.post = lambda *args, **kwargs: None
    sys.modules.setdefault("requests", requests_mod)

    yaml_mod = types.ModuleType("yaml")
    yaml_mod.safe_load = lambda raw: {}
    sys.modules.setdefault("yaml", yaml_mod)

    psycopg_mod = types.ModuleType("psycopg")
    psycopg_mod.connect = lambda *args, **kwargs: None
    psycopg_rows_mod = types.ModuleType("psycopg.rows")
    psycopg_rows_mod.dict_row = object()
    sys.modules.setdefault("psycopg", psycopg_mod)
    sys.modules.setdefault("psycopg.rows", psycopg_rows_mod)


_install_import_stubs()
pg_vacman = importlib.import_module("pg_vacman")


def minimal_config():
    return {
        "db": {
            "host": "127.0.0.1",
            "port": 5432,
            "dbname": "postgres",
            "user": "postgres",
            "password": "",
            "connect_timeout_sec": 5,
            "application_name": "pg_vacman",
        },
        "targets": {"max_databases_per_run": 0},
        "run": {
            "advisory_lock_key": 90421001,
            "json_detail_level": "verbose",
            "json_max_skips_per_db": 30,
            "notify_max_actions_per_db": 30,
            "skip_history_enabled": False,
            "skip_history_threshold": 3,
        },
        "thresholds": {
            "min_table_size_mb": 64,
            "min_dead_ratio": 0.15,
            "max_analyze_age_hours": 72,
            "max_last_vacuum_age_hours": 168,
            "freeze_age_threshold": 1500000000,
            "vacuum_full": {
                "enabled": False,
                "start": "01:00",
                "end": "05:00",
                "min_dead_ratio": 0.6,
                "min_table_size_mb": 2048,
                "min_estimated_dead_mb": 0,
                "min_estimated_dead_ratio": 0,
            },
        },
        "limits": {
            "max_tables_per_db": 30,
            "max_actions_global": 200,
            "parallel_tables_per_db": 1,
            "global_parallel_limit": 2,
            "sleep_between_tables_sec": 0,
            "sleep_between_databases_sec": 0,
            "lock_timeout_ms": 2000,
            "per_table_statement_timeout_sec": 1800,
            "vacuum_cost_delay_ms": 0,
            "vacuum_cost_limit": 0,
        },
        "force": {"enabled": False, "default_action": "ANALYZE", "tables": []},
        "vacuum_full_policy": {"enabled": True, "allow_objects": [], "on_miss": "VACUUM_ANALYZE"},
        "retry": {"enabled": True, "max_attempts": 2, "base_sleep_ms": 300, "max_sleep_ms": 3000, "jitter_ms": 0},
        "metrics": {"enabled": False},
    }


class RuntimeContractTests(unittest.TestCase):
    def test_validate_config_accepts_minimal_config(self):
        pg_vacman.validate_config(minimal_config())

    def test_validate_config_rejects_bad_detail_level(self):
        cfg = minimal_config()
        cfg["run"]["json_detail_level"] = "too-much"
        with self.assertRaises(ValueError):
            pg_vacman.validate_config(cfg)

    def test_resolve_exit_code_json_failure(self):
        summary = {"aborted": False, "json_save_failed": True, "global": {"json_fail_on_error": True}}
        self.assertEqual(pg_vacman.resolve_exit_code(summary), 7)

    def test_resolve_exit_code_skip_history_alert(self):
        summary = {
            "aborted": False,
            "json_save_failed": False,
            "global": {"executed_fail": 0, "json_fail_on_error": True},
            "skip_history": {"fail_on_threshold": True, "alert_count": 1},
            "db_results": [],
        }
        self.assertEqual(pg_vacman.resolve_exit_code(summary), 8)

    def test_skip_history_alerts_after_threshold(self):
        cfg = minimal_config()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "skip_history.json"
            cfg["run"].update(
                {
                    "skip_history_enabled": True,
                    "skip_history_path": str(path),
                    "skip_history_threshold": 2,
                    "skip_history_fail_on_threshold": True,
                }
            )
            summary = {
                "db_results": [
                    {
                        "actions": [
                            {
                                "db": "appdb",
                                "table": "public.orders",
                                "action": "VACUUM_ANALYZE",
                                "status": "SKIP",
                                "skip_reason": "lock_timeout",
                            }
                        ]
                    }
                ]
            }
            now = dt.datetime(2026, 6, 30, tzinfo=dt.timezone.utc)
            pg_vacman.update_skip_history(cfg, summary, now)
            self.assertEqual(summary["skip_history"]["alert_count"], 0)
            pg_vacman.update_skip_history(cfg, summary, now)
            self.assertEqual(summary["skip_history"]["alert_count"], 1)

    def test_prometheus_textfile_metrics(self):
        cfg = minimal_config()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "pg_vacman.prom"
            cfg["metrics"] = {"enabled": True, "prometheus_textfile": str(path)}
            summary = {
                "aborted": False,
                "json_save_failed": False,
                "global": {"planned_actions": 3, "executed_ok": 2, "executed_skip": 1, "executed_fail": 0, "skipped_dbs": 0},
                "skip_history": {"alert_count": 0},
            }
            pg_vacman.write_metrics(cfg, summary, 0)
            text = path.read_text(encoding="utf-8")
            self.assertIn('pg_vacman_run_metric{metric="planned_actions"} 3', text)
            self.assertIn('pg_vacman_run_metric{metric="exit_code"} 0', text)


if __name__ == "__main__":
    unittest.main()
