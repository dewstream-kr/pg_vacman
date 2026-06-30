# pg_vacman Operations Manual

This manual is for DBAs and operators running `pg_vacman` from cron, systemd,
or another scheduler.

## Scope

`pg_vacman` is a PostgreSQL maintenance runner. It plans and optionally executes:

- `ANALYZE`
- `VACUUM (ANALYZE)`
- `VACUUM (FREEZE, ANALYZE)`
- `VACUUM (FULL, ANALYZE)` when explicitly enabled and allowed

It is not a replacement for PostgreSQL autovacuum. Use it as a controlled,
policy-based maintenance pass for selected databases and tables.

## Entrypoint Choice

Use `pg_vacman.py` on Python 3.7+ systems. This is the preferred entrypoint.

Use `pg_vacman_py36.py` only for legacy Python 3.6 hosts. The compatibility
entrypoint keeps older dependency constraints while using the same `OK` /
`SKIP` / `FAIL` status contract for run summaries.

## Pre-Deployment Checklist

1. Copy `config.yaml` to `config.local.yaml`.
2. Keep `config.local.yaml` out of Git. It is ignored by `.gitignore`.
3. Set database connection values under `db`.
4. Prefer explicit `targets.include_databases` for production.
5. Keep `run.dry_run_default: true` until the first reviewed dry run succeeds.
6. Keep `thresholds.vacuum_full.enabled: false` unless there is an approved
   maintenance window.
7. Keep `vacuum_full_policy.enabled: true`.
8. Start with `limits.parallel_tables_per_db: 1`.
9. Start with `limits.global_parallel_limit: 1` or `2`.
10. Enable JSON output and retain run reports for audit history.
11. Keep skip history enabled unless table-level skips are expected and reviewed.
12. Enable metrics export if your host has a Prometheus textfile collector or
    StatsD receiver.

## Recommended First Run

Run planning only:

```bash
python3 pg_vacman.py --config config.local.yaml --dry-run
```

Review:

- target databases
- planned actions
- skipped databases
- skipped objects in verbose JSON
- any `VACUUM_FULL_ANALYZE` downgrade or skip

Then run apply mode only after the plan is expected:

```bash
python3 pg_vacman.py --config config.local.yaml --apply
```

## Advisory Lock Behavior

The runner uses `pg_try_advisory_lock()` with `run.advisory_lock_key`.

The current implementation keeps a dedicated `control_lock` database connection
open for the full run. This means the session-level advisory lock covers:

- database discovery
- candidate planning
- table maintenance execution
- JSON report writing
- notifications
- final cleanup

If another process already holds the same advisory lock key, the run exits with
code `2`.

## Concurrency Behavior

Databases are processed sequentially.

Within the active database:

- `limits.parallel_tables_per_db` sets the worker pool size.
- `limits.global_parallel_limit` is enforced with a semaphore and caps concurrent
  maintenance workers for the run.

For production, keep both low until you have measured I/O, lock waits, and
replication impact.

## Safety Settings

Worker sessions apply:

- `lock_timeout`
- `statement_timeout`
- `vacuum_cost_delay`
- `vacuum_cost_limit`

These settings protect normal workload latency during table maintenance.
Candidate discovery still performs catalog/statistics queries and can be
expensive on very large clusters, especially because it calls
`pg_total_relation_size()` for candidate ordering.

Control and candidate discovery sessions now apply the same session safety
settings, including `statement_timeout`, before catalog/statistics queries.

## VACUUM FULL Policy

`VACUUM FULL` is intentionally guarded by two layers:

1. `thresholds.vacuum_full.enabled`
2. `vacuum_full_policy`

Recommended production posture:

```yaml
thresholds:
  vacuum_full:
    enabled: false

vacuum_full_policy:
  enabled: true
  allow_objects: []
  on_miss: "VACUUM_ANALYZE"
  force_bypass: false
```

Use `allow_objects` only for tables approved for rewrite during a controlled
window. Remember that `VACUUM FULL` takes an `ACCESS EXCLUSIVE` lock and rewrites
the table.

Optional `min_estimated_dead_mb` and `min_estimated_dead_ratio` settings can add
an estimated dead-space condition based on `pg_stats.avg_width * n_dead_tup`.
Treat these values as planner hints, not exact bloat measurements.

## Exit Codes

Schedulers should use the process exit code:

| Code | Meaning |
| ---: | --- |
| `0` | Success, or table-level skips only |
| `1` | Failed to list target databases |
| `2` | Another run already holds the advisory lock |
| `3` | Failed to connect to the control database |
| `4` | One or more table actions failed |
| `5` | Run was aborted by graceful or immediate stop |
| `6` | One or more databases were skipped for a non-standby reason |
| `7` | JSON report writing failed and `run.json_fail_on_error` is enabled |
| `8` | Repeated SKIP history reached `run.skip_history_threshold` |

Table-level skips such as precheck conflicts or lock timeouts are recorded in
the report and notification output. A single table-level skip does not fail the
process by default, but repeated skips can fail the process when skip history
alerting is enabled.

## Result Interpretation

Modern action results use:

- `OK`: maintenance completed
- `SKIP`: intentionally skipped, usually due to stop signal, precheck, or a
  deferrable PostgreSQL condition
- `FAIL`: failed and was not classified as a skip

Legacy `ok` and `skipped` flags are retained for compatibility, but `status`
is the canonical field.

## Repeated SKIP History

When `run.skip_history_enabled: true`, the runner stores repeated table-level
skip observations in `run.skip_history_path`.

The key is:

```text
database | schema.table | action | skip reason
```

When the count reaches `run.skip_history_threshold`, the run summary includes
`skip_history.alerts`. If `run.skip_history_fail_on_threshold: true`, the
process exits with code `8`.

If `run.skip_history_reset_on_ok: true`, a later successful action for the same
database/table/action clears previous skip entries for that action.

## JSON Reports

When `run.json_auto_save: true`, a run report is written under
`run.json_out_dir` unless `--json-out` is provided.

The report is useful for:

- audit history
- action-level troubleshooting
- lock or timeout diagnosis
- trend analysis outside the tool

If JSON report writing fails, the current version logs `json_save_failed`. With
`run.json_fail_on_error: true`, the process exits with code `7`.

## Metrics

Metrics export is best-effort and does not fail the run.

Supported outputs:

- Prometheus node_exporter textfile collector
- StatsD UDP gauge metrics

Example metrics include:

- planned actions
- OK/SKIP/FAIL counts
- skipped database count
- JSON save failure flag
- repeated SKIP alert count
- final exit code

## Notifications

Slack and Telegram notification failures do not fail the run. They are logged as
warnings.

Keep `run.notify_include_sql: false` unless debugging, because SQL text can make
messages noisy.

## Stop Handling

- Press `Ctrl+C` once for graceful stop.
- Press `Ctrl+C` twice, or send `SIGTERM`, for immediate stop.

Immediate stop attempts to cancel and close active worker connections.

## Cron Pattern

Example:

```cron
0 2 * * * /usr/bin/python3 /opt/pg_vacman/pg_vacman.py \
  --config /opt/pg_vacman/config.local.yaml \
  --apply >> /var/log/pg_vacman/apply.log 2>&1
```

Recommended cron practices:

- Use absolute paths.
- Capture stdout and stderr.
- Monitor non-zero exit codes.
- Rotate logs.
- Archive JSON reports.

## Troubleshooting

`exit=2`

Another run is active or a previous session is still connected. Check
`pg_stat_activity` for `application_name` containing `pg_vacman:control_lock`.

`exit=3`

The control connection failed. Check host, port, database, role, password,
`pg_hba.conf`, network reachability, and TLS requirements.

`exit=4`

At least one table action failed. Review the JSON report for `FAIL` actions and
their PostgreSQL error details.

`exit=5`

The run was stopped by a signal. Review whether it was a planned stop or an
external scheduler timeout.

`exit=6`

One or more databases were skipped for a non-standby reason. Common causes are
candidate discovery failures or global action limit exhaustion.

`exit=7`

JSON report writing failed. Check report directory permissions and free space.

`exit=8`

Repeated table-level SKIP history reached the configured threshold. Review
`skip_history.alerts` in the run report and the history file.

## Security

- Do not commit real passwords, webhook URLs, or bot tokens.
- Use `config.local.yaml` or an environment-specific config file.
- Prefer a dedicated PostgreSQL role with only the privileges needed for
  connection, catalog/statistics reads, and maintenance on target tables.
- Restrict access to JSON reports if table names or operational metadata are
  sensitive.
