# pg_vacman

`pg_vacman` is a lightweight PostgreSQL maintenance manager that plans and runs  
`ANALYZE`, `VACUUM (ANALYZE)`, `VACUUM (FREEZE, ANALYZE)`, and optionally  
`VACUUM (FULL, ANALYZE)` across **multiple databases** in a PostgreSQL cluster.

It discovers target databases from `pg_database` (or uses an explicit list),
filters tables by schema and object patterns, decides actions using configurable
thresholds, and executes with per-database and global concurrency limits.

It also supports:

- JSON run reports (basic or verbose)
- Slack and Telegram notifications (optional)
- Graceful and immediate stop signals
- Best-effort prechecks for conflicting maintenance activity
- Retry and backoff for retryable failures
- VACUUM FULL safety guardrails

For operator-focused runbooks, see [`OPERATIONS_MANUAL.md`](OPERATIONS_MANUAL.md)
or the Korean version [`OPERATIONS_MANUAL_KO.md`](OPERATIONS_MANUAL_KO.md).

---

## Features

- Multi-database maintenance loop
- Schema and object filtering with glob support
- Threshold-based action planning
- Optional force rules
- Advisory lock for singleton runs
- Per-database and global concurrency limits
- Graceful stop and immediate stop support
- JSON summary with optional verbose detail
- Slack / Telegram notifications
- Precheck support for autovacuum / vacuum / analyze conflict avoidance
- Retry / backoff policy for retryable failures
- VACUUM FULL allowlist guardrail with downgrade / skip behavior
- Separate tracking of successful, failed, and skipped actions
- Exit codes for cron, systemd, and CI integration
- Session-level advisory lock held for the full run to prevent overlapping runs

---

## Entrypoints

- `pg_vacman.py` is the recommended modern entrypoint for Python 3.7+.
- `pg_vacman_py36.py` is a legacy compatibility entrypoint for Python 3.6-era
  hosts such as RHEL/CentOS 7.

The modern entrypoint has the most detailed execution metadata, including
explicit `OK` / `SKIP` / `FAIL` status fields in action results. The Python 3.6
entrypoint now uses the same status contract for run summaries, while retaining
legacy-compatible implementation details.

---

## Requirements

### Python Versions

- **Python 3.7 or later** (recommended)
- Python 3.6 / OpenSSL 1.0.2k supported via `pg_vacman_py36.py` + `requirements_py36.txt`

### Runtime Dependencies

#### Modern build (`pg_vacman.py`)

- PostgreSQL driver:
  - `psycopg` (v3 preferred)
  - fallback: `psycopg2-binary`
- YAML configuration loader:
  - `PyYAML>=6`
- HTTP client for notifications:
  - optional: `requests`

#### Legacy build (`pg_vacman_py36.py`)

- PostgreSQL driver:
  - `psycopg2-binary==2.9.6`
- YAML configuration loader:
  - `PyYAML==6.0.1`
- HTTP client for notifications:
  - optional: `requests==2.27.1`
  - when `requests` is unavailable or unusable, the script falls back to stdlib `urllib`

### PostgreSQL Privileges

The configured database user must be able to:

- CONNECT to target databases
- Read system catalogs and statistics (for example `pg_stat_user_tables`)
- Run `ANALYZE` and `VACUUM` on target tables

> **VACUUM FULL warning**  
> `VACUUM FULL` takes **ACCESS EXCLUSIVE** locks and rewrites tables.  
> It can block reads and writes. Keep it disabled unless you have a controlled maintenance window.

---

## Installation

### Create a Virtual Environment (Recommended)

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
```

### Install Dependencies

#### Modern Python (Recommended)

Using `requirements.txt`:

```txt
psycopg[binary]>=3.1,<4.0
psycopg2-binary>=2.9,<3.0
PyYAML>=6.0,<7.0
requests>=2.25,<3.0
dataclasses; python_version < "3.7"
```

Install:

```bash
pip install -r requirements.txt
```

#### Manual Installation

```bash
pip install "psycopg[binary]>=3.1,<4.0" \
            "psycopg2-binary>=2.9,<3.0" \
            "PyYAML>=6.0,<7.0" \
            "requests>=2.25,<3.0"
```

#### Legacy Python 3.6 / RHEL7 / CentOS7

Use the compatibility entrypoint and pinned dependencies:

```bash
python3.6 -m pip install -r requirements_py36.txt
python3.6 -c "import psycopg2, yaml; print('OK')"
python3.6 pg_vacman_py36.py --config config.local.yaml --dry-run
```

---

## Quick Start

### 1. Copy the Example Configuration

```bash
cp config.yaml config.local.yaml
```

`config.local.yaml` is intended for environment-specific settings and is ignored
by Git.

### 2. Edit Configuration

At minimum, configure:

- `db.host`
- `db.port`
- `db.dbname`
- `db.user`
- `db.password`

Other commonly customized sections:

- `targets.*`
- `filters.*`
- `thresholds.*`
- `limits.*`
- `precheck.*`
- `vacuum_full_policy.*`
- `retry.*`
- `notify.*`

### 3. Dry Run (Plan Only)

No SQL is executed.

```bash
python3 pg_vacman.py --config config.local.yaml --dry-run

# Legacy (Python 3.6):
# python3.6 pg_vacman_py36.py --config config.local.yaml --dry-run
```

### 4. Apply (Execute Actions)

```bash
python3 pg_vacman.py --config config.local.yaml --apply

# Legacy (Python 3.6):
# python3.6 pg_vacman_py36.py --config config.local.yaml --apply
```

### 5. JSON Run Report

If `run.json_auto_save: true` and `--json-out` is not provided,
a JSON summary is automatically saved to:

```text
run.json_out_dir/run_YYYYMMDD_HHMMSS.json
```

To specify an explicit path:

```bash
python3 pg_vacman.py \
  --config config.local.yaml \
  --apply \
  --json-out ./runs/run.json

# Legacy (Python 3.6):
# python3.6 pg_vacman_py36.py --config config.local.yaml --apply --json-out ./runs/run.json
```

---

## Configuration Reference

### `db`

Database connection settings used for both **control** and **worker** sessions.

- `host`  
  PostgreSQL server hostname or IP address

- `port`  
  PostgreSQL server port

- `dbname`  
  Control database used for advisory locking and database discovery  
  (typically `postgres`)

- `user`  
  Database username

- `password`  
  Database user password

- `connect_timeout_sec`  
  Connection timeout in seconds

- `application_name`  
  Base PostgreSQL `application_name`  
  (suffixes are automatically added for control / worker sessions)

---

### `targets`

Controls which databases are processed during a run.

- `include_databases`  
  Explicit list of databases to process  
  (if empty, all eligible databases are considered)

- `exclude_databases`  
  Databases to skip

- `exclude_templates`  
  Skip template databases (`template0`, `template1`)

- `require_allow_conn`  
  Only include databases where `datallowconn = true`

- `max_databases_per_run`  
  Maximum number of databases processed per run  
  (`0` = no limit)

- `primary_only`  
  Skip standby databases  
  (checked via `pg_is_in_recovery()`)

---

### `run`

Controls overall run behavior.

- `advisory_lock_key`  
  Session-level PostgreSQL advisory lock key used to prevent overlapping runs.
  The lock connection is kept open until final cleanup, so the lock covers the
  full database discovery, planning, execution, report, and notification flow.

- `timezone`  
  Timezone used for timestamps and time-window checks

- `dry_run_default`  
  Default execution mode when no CLI flag is provided

- `log_level`  
  Logging verbosity (`debug`, `info`, `warning`, `error`)

- `json_detail_level`  
  JSON output detail level (`basic` or `verbose`)

- `json_max_skips_per_db`  
  Maximum number of skipped objects recorded per database in verbose mode

- `json_auto_save`  
  Automatically save a JSON run report

- `json_fail_on_error`
  Return a non-zero exit code when JSON report writing fails

- `json_out_dir`, `json_out_prefix`  
  Output directory and filename prefix for JSON reports

- `notify_max_actions_per_db`  
  Maximum number of table-level actions shown per database in notifications

- `notify_include_sql`  
  Include SQL statements in notifications  
  (may significantly increase message length)

- `skip_history_enabled`, `skip_history_path`
  Track repeated table-level `SKIP` results across runs

- `skip_history_threshold`
  Number of repeated skips for the same database/table/action/reason before an alert is raised

- `skip_history_fail_on_threshold`
  Return a non-zero exit code when skip history alerts are present

- `skip_history_reset_on_ok`
  Clear previous skip history for a table/action after a successful action

---

### `thresholds`

Planner thresholds used to decide maintenance actions.

- `min_table_size_mb`  
  Skip tables smaller than this size (in MB)

- `min_dead_ratio`  
  Minimum dead tuple ratio required to consider vacuuming

- `max_analyze_age_hours`  
  Treat `ANALYZE` as stale after this many hours

- `max_last_vacuum_age_hours`  
  Treat `VACUUM` as stale after this many hours

- `freeze_age_threshold`  
  Transaction ID age threshold for freeze consideration

- `vacuum_full`  
  Sub-configuration block for optional `VACUUM FULL` planning  
  (time window, size threshold, dead tuple ratio, optional estimated dead-space threshold)

> `thresholds.vacuum_full.enabled` controls whether the planner may ever choose
> `VACUUM FULL`.  
> Even when enabled, `vacuum_full_policy` can still downgrade or skip it.

`thresholds.vacuum_full.min_estimated_dead_mb` and
`thresholds.vacuum_full.min_estimated_dead_ratio` are optional guardrails based
on `pg_stats.avg_width * n_dead_tup`. They are estimates, not exact bloat
measurements, and default to `0` so existing behavior is unchanged.

---

### `limits`

Execution throttles and session safety settings.

- `max_tables_per_db`  
  Maximum number of tables processed per database

- `max_actions_global`  
  Maximum number of maintenance actions per run (global)

- `parallel_tables_per_db`  
  Number of concurrent table workers per database

- `global_parallel_limit`  
  Global concurrency limit across all databases

- `sleep_between_tables_sec`  
  Delay between table executions (seconds)

- `sleep_between_databases_sec`  
  Delay between databases (seconds)

- `lock_timeout_ms`  
  PostgreSQL `lock_timeout` in milliseconds

- `per_table_statement_timeout_sec`  
  Statement timeout per table (seconds)

- `vacuum_cost_delay_ms`  
  Optional `vacuum_cost_delay` setting

- `vacuum_cost_limit`  
  Optional `vacuum_cost_limit` setting

---

### `filters`

Controls which tables are eligible for maintenance.

- `include_schemas`  
  Only include these schemas (if non-empty)

- `exclude_schemas`  
  Schemas to skip

- `include_objects`  
  Object-level whitelist patterns  
  (format: `db:schema.table`, glob patterns supported)

- `exclude_objects`  
  Object-level blacklist patterns  
  (takes precedence over include rules)

- `include_tables` *(legacy)*  
  Legacy table patterns (automatically merged into `include_objects`)

- `exclude_tables` *(legacy)*  
  Legacy table exclusion patterns

---

### `force`

Overrides threshold-based decision logic when patterns match.

- `enabled`  
  Enable or disable force rules

- `default_action`  
  Default action applied when a forced rule specifies no action

- `tables`  
  List of force rules  
  (pattern format: `db:schema.table`, optional explicit action)

> Force rules can still be constrained by `vacuum_full_policy` when the forced
> action is `VACUUM_FULL_ANALYZE`, unless `vacuum_full_policy.force_bypass = true`.

---

### `precheck`

Best-effort conflict avoidance before executing maintenance.

- `enabled`  
  Enable or disable precheck logic

- `skip_if_autovacuum_running`  
  Skip the target if autovacuum or autoanalyze is already active on the same relation

- `skip_if_vacuum_running`  
  Skip the target if another regular `VACUUM` is already active on the same relation

- `skip_if_analyze_running`  
  Skip the target if another `ANALYZE` is already active on the same relation

- `skip_vacuum_full_if_relation_locked`  
  Skip `VACUUM FULL` if the relation is already locked by another backend

> Precheck is **best-effort**.  
> The modern build uses progress views and relation lock checks where available.  
> The Python 3.6 compatibility build uses `pg_stat_progress_vacuum`, lock checks,
> and `pg_stat_activity` query matching as a fallback.

---

### `vacuum_full_policy`

Additional safety controls for `VACUUM FULL`.

- `enabled`  
  Enable or disable the policy

- `allow_objects`  
  Allowlist of object patterns permitted to run `VACUUM FULL`

- `on_miss`  
  Action taken when a `VACUUM FULL` target is not allowlisted:
  - `VACUUM_ANALYZE`
  - `SKIP`

- `force_bypass`  
  If `true`, a matching `force` rule may bypass the allowlist

This section is useful when threshold logic or force rules could otherwise produce
a `VACUUM FULL` plan for a table that should never be rewritten in production.

---

### `retry`

Retry and backoff policy for retryable failures.

- `enabled`  
  Enable or disable retries

- `max_attempts`  
  Maximum execution attempts per table action

- `base_sleep_ms`  
  Base retry sleep time in milliseconds

- `max_sleep_ms`  
  Maximum retry sleep time in milliseconds

- `jitter_ms`  
  Random jitter added to reduce retry synchronization

- `retryable_categories`  
  Error categories that may be retried, such as:
  - `lock_timeout`
  - `lock_not_available`
  - `statement_timeout`
  - `deadlock_detected`
  - `serialization_failure`
  - `query_canceled`

- `overrides`  
  Optional override rules applied by database pattern, action pattern, and time window

This is especially useful when you want:

- no retries during business hours
- stronger retry behavior during maintenance windows
- very limited retry for `VACUUM FULL`

---

### `notify`

Notification settings.

- `slack_webhook_url`  
  Slack Incoming Webhook URL

- `telegram_bot_token`  
  Telegram bot token

- `telegram_chat_id`  
  Telegram chat ID

---

### `metrics`

Best-effort metrics export settings.

- `enabled`
  Enable or disable metrics export

- `prometheus_textfile`
  Optional Prometheus node_exporter textfile collector path

- `statsd_host`, `statsd_port`, `statsd_prefix`
  Optional StatsD UDP target and metric prefix

Metrics failures are logged as warnings and do not fail the run.

---

## Decision Logic

For each table that passes all filters, `pg_vacman` evaluates maintenance needs
in the following order:

1. **Size check**
   - Skip the table if its size is smaller than `thresholds.min_table_size_mb`

2. **Threshold evaluation**
   - Dead tuple ratio (`dead_ratio`)
   - Time since last `VACUUM` or `ANALYZE`
   - Transaction ID freeze age

3. **Initial action selection**
   - `ANALYZE`
   - `VACUUM_ANALYZE`
   - `VACUUM_FREEZE_ANALYZE`
   - `VACUUM_FULL_ANALYZE`
   - `SKIP`

4. **Force rule override**
   - If a matching `force` rule exists, threshold-based logic is bypassed

5. **VACUUM FULL policy adjustment**
   - If `vacuum_full_policy.enabled = true`, a planned `VACUUM FULL` may be:
     - allowed
     - downgraded to `VACUUM_ANALYZE`
     - skipped

6. **Execution-time precheck**
   - Before execution, the worker may skip the action if conflicting maintenance is already running

7. **Retry loop**
   - Retryable failures may be retried according to `retry` policy

---

## Result States

Each table action ends in one of the following states:

- **OK**  
  The maintenance action completed successfully

- **FAIL**  
  The action failed and was not skipped or retried successfully

- **SKIP**  
  The action was intentionally not executed, for example due to:
  - graceful / immediate stop
  - precheck conflict
  - policy-based skip
  - retry exhaustion for an action treated as skipped

This separation is useful for operations dashboards and alerting,
because a skipped action is often operationally different from a true failure.

Compatibility note:

- Both entrypoints store explicit action `status` values.
- Legacy `ok` / `skipped` flags are retained for compatibility.

---

## Exit Codes

`pg_vacman` returns process exit codes suitable for cron, systemd, and CI jobs:

- `0` success or table-level skips only
- `1` failed to list target databases
- `2` another run is already active for the advisory lock key
- `3` failed to connect to the control database
- `4` one or more table actions failed
- `5` the run was aborted by a graceful or immediate stop signal
- `6` one or more databases were skipped for a non-standby reason
- `7` JSON report writing failed and `run.json_fail_on_error` is enabled
- `8` repeated SKIP history reached `run.skip_history_threshold`

Table-level `SKIP` results, such as lock timeout or precheck skip, remain visible
in the JSON report and notifications but do not make the process fail by default.
Repeated table-level skips can fail the process when skip history alerting is
enabled. JSON report writing failures fail the process when
`run.json_fail_on_error` is enabled.

---

## Concurrency Model

Maintenance execution is controlled by two independent limits:

- **Per-database parallelism**
  - Controlled by `limits.parallel_tables_per_db`
  - Limits how many tables are processed concurrently within a single database

- **Global concurrency cap**
  - Controlled by `limits.global_parallel_limit`
  - Caps concurrent maintenance workers for the run

Databases are processed sequentially in the current implementation. Within the
active database, a per-database worker pool is constrained by the global
semaphore so the configured global cap remains a hard upper bound.

---

## Signals

`pg_vacman` supports graceful and immediate termination:

- **Ctrl+C (once)**  
  Graceful stop.
  No new tasks are started, and running tasks are allowed to finish.

- **Ctrl+C (twice) or SIGTERM**  
  Immediate stop.
  Active queries are cancelled and connections are closed.

---

## Notifications

If enabled, notification messages include:

- Global execution summary
- Per-database results
- Per-table action results (bounded by configuration limits)

The following notification channels are supported:

- Slack (Incoming Webhook)
- Telegram (Bot API)

For Python 3.6 environments, notifications still work even if `requests` is unavailable,
because the compatibility build can fall back to stdlib `urllib`.

---

## JSON Run Report

Each run can produce a structured JSON report containing:

- Run metadata (timestamp, mode, configuration snapshot)
- Global execution summary
- Per-database summaries
- Per-action execution details
- Verbose decision context (when enabled)
- Retry attempt history
- Precheck details (when available)

This report can be used for auditing, troubleshooting, and historical analysis.

---

## Safe Operating Recommendations

For initial or production use:

- Keep `thresholds.vacuum_full.enabled` set to `false`
- Enable `vacuum_full_policy.enabled` for guardrail protection
- Use `vacuum_full_policy.allow_objects` only for explicitly approved targets
- Enable `precheck.enabled` to avoid collisions with autovacuum
- Start with conservative concurrency:
  - `parallel_tables_per_db: 1`
  - `global_parallel_limit: 1` or `2`
- Prefer limited retry behavior:
  - disable or reduce retry during business hours
  - allow slightly stronger retry during night maintenance windows
- Always validate behavior using `--dry-run` before applying changes

---

## Security Notes

- Never commit real database passwords or webhook URLs
- Treat `config.yaml` as a sample configuration only
- Use environment-specific configuration files for real deployments
- `config.local.yaml`, local run reports, logs, virtual environments, and
  Python cache files are ignored by `.gitignore`
- Restrict access to JSON reports if database names, table names, or operational
  metadata are sensitive in your environment

---

## Cron Example

Below is an example of running `pg_vacman` periodically using `cron`.
This is useful for unattended maintenance in production environments.

### Basic Daily Run (Dry Run)

Runs every day at **01:00**, planning only (no SQL execution):

```cron
0 1 * * * /usr/bin/python3 /opt/pg_vacman/pg_vacman.py \
  --config /opt/pg_vacman/config.local.yaml \
  --dry-run >> /var/log/pg_vacman/dry_run.log 2>&1
```

### Daily Apply Run (Recommended Pattern)

Runs every day at **02:00**, executing actions:

```cron
0 2 * * * /usr/bin/python3 /opt/pg_vacman/pg_vacman.py \
  --config /opt/pg_vacman/config.local.yaml \
  --apply >> /var/log/pg_vacman/apply.log 2>&1
```

### Legacy Python 3.6 Apply Run

```cron
0 2 * * * /usr/bin/python3.6 /opt/pg_vacman/pg_vacman_py36.py \
  --config /opt/pg_vacman/config.local.yaml \
  --apply >> /var/log/pg_vacman/apply_py36.log 2>&1
```

### Recommended Cron Practices

- Always use absolute paths (`/usr/bin/python3`, `/opt/...`)
- Redirect stdout and stderr to log files
- Start with `--dry-run` when introducing new rules
- Rely on `run.advisory_lock_key` to prevent overlapping executions
- Keep `VACUUM FULL` disabled unless running in a controlled window

### Example Log Rotation (Optional)

```cron
0 0 * * * /usr/sbin/logrotate /etc/logrotate.d/pg_vacman
```

---

## License

This repository is licensed under the **GNU General Public License v3.0 or later (GPL-3.0-or-later)**.
