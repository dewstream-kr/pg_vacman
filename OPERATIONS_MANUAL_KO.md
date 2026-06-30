# pg_vacman 운영 매뉴얼

이 문서는 `pg_vacman`을 cron, systemd, 또는 별도 스케줄러에서 운영하는 DBA와 운영자를 위한 매뉴얼입니다.

## 범위

`pg_vacman`은 PostgreSQL 유지보수 실행기입니다. 설정과 통계 정보를 기준으로 다음 작업을 계획하고, 필요하면 실행합니다.

- `ANALYZE`
- `VACUUM (ANALYZE)`
- `VACUUM (FREEZE, ANALYZE)`
- 명시적으로 허용된 경우의 `VACUUM (FULL, ANALYZE)`

이 도구는 PostgreSQL autovacuum을 대체하지 않습니다. 운영자가 정의한 정책에 따라 특정 DB와 테이블을 통제된 방식으로 점검하고 보강하는 용도로 사용해야 합니다.

## 엔트리포인트 선택

Python 3.7 이상 환경에서는 `pg_vacman.py`를 사용합니다. 이 파일이 권장 엔트리포인트입니다.

Python 3.6 기반의 레거시 서버, 예를 들어 RHEL/CentOS 7 계열에서는 `pg_vacman_py36.py`를 사용합니다. Python 3.6판도 현재 버전에서는 실행 결과를 `OK` / `SKIP` / `FAIL` 상태 계약으로 기록합니다.

## 배포 전 체크리스트

1. `config.yaml`을 `config.local.yaml`로 복사합니다.
2. `config.local.yaml`은 Git에 커밋하지 않습니다. `.gitignore`에 포함되어 있습니다.
3. `db` 섹션에 접속 대상, 포트, DB명, 사용자 정보를 설정합니다.
4. 운영 환경에서는 명시적인 대상 DB 목록을 우선 사용합니다.
5. 첫 검토용 dry-run이 성공할 때까지 `run.dry_run_default: true`를 유지합니다.
6. 승인된 점검 시간이 없으면 `thresholds.vacuum_full.enabled: false`를 유지합니다.
7. `vacuum_full_policy.enabled: true`를 유지합니다.
8. 최초 운영 시 `limits.parallel_tables_per_db: 1`부터 시작합니다.
9. 최초 운영 시 `limits.global_parallel_limit`은 `1` 또는 `2`부터 시작합니다.
10. JSON 리포트를 활성화하고 감사 이력으로 보관합니다.
11. 반복 SKIP 이력 추적은 특별한 사유가 없으면 활성화합니다.
12. Prometheus textfile collector 또는 StatsD 수신기가 있으면 메트릭 출력을 활성화합니다.

## 최초 실행 절차

먼저 계획만 확인합니다.

```bash
python3 pg_vacman.py --config config.local.yaml --dry-run
```

검토 항목은 다음과 같습니다.

- 대상 DB 목록
- 계획된 작업
- SKIP된 DB
- verbose JSON에 기록된 SKIP 객체
- `VACUUM_FULL_ANALYZE`가 downgrade 또는 skip 되었는지 여부

계획이 예상과 일치할 때만 apply 모드로 실행합니다.

```bash
python3 pg_vacman.py --config config.local.yaml --apply
```

## Advisory Lock 동작

실행기는 `run.advisory_lock_key` 값을 사용해 `pg_try_advisory_lock()`을 획득합니다.

현재 구현은 `control_lock` 전용 DB 세션을 전체 실행 동안 유지합니다. 따라서 세션 레벨 advisory lock은 다음 범위를 모두 보호합니다.

- 대상 DB 탐색
- candidate 계획 수립
- 테이블 유지보수 실행
- JSON 리포트 저장
- 알림 전송
- 최종 정리

다른 프로세스가 같은 advisory lock key를 이미 보유하고 있으면 프로세스는 exit code `2`로 종료됩니다.

## 동시성 모델

DB는 순차적으로 처리됩니다.

현재 처리 중인 DB 안에서는 다음 설정이 적용됩니다.

- `limits.parallel_tables_per_db`: DB 내부 worker pool 크기
- `limits.global_parallel_limit`: 전체 실행 기준 동시 maintenance worker 상한

운영 환경에서는 I/O, lock wait, replication 영향도를 측정하기 전까지 두 값을 낮게 유지하는 것이 안전합니다.

## 안전 설정

worker 세션에는 다음 설정이 적용됩니다.

- `lock_timeout`
- `statement_timeout`
- `vacuum_cost_delay`
- `vacuum_cost_limit`

이 설정은 테이블 유지보수 작업이 일반 workload latency에 주는 영향을 줄이기 위한 보호 장치입니다.

candidate 탐색도 catalog/statistics 쿼리를 수행합니다. 특히 정렬과 판단을 위해 `pg_total_relation_size()`를 호출하므로 대형 클러스터에서는 비용이 커질 수 있습니다. 현재 버전은 control 및 candidate discovery 세션에도 `statement_timeout`을 포함한 세션 안전 설정을 적용합니다.

## VACUUM FULL 정책

`VACUUM FULL`은 의도적으로 두 단계 가드레일을 거칩니다.

1. `thresholds.vacuum_full.enabled`
2. `vacuum_full_policy`

운영 권장 기본값은 다음과 같습니다.

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

`allow_objects`에는 통제된 점검 시간에 rewrite가 승인된 테이블만 넣습니다. `VACUUM FULL`은 `ACCESS EXCLUSIVE` lock을 잡고 테이블을 rewrite하므로 읽기와 쓰기를 모두 차단할 수 있습니다.

`min_estimated_dead_mb`와 `min_estimated_dead_ratio`를 사용하면 `pg_stats.avg_width * n_dead_tup` 기반의 추정 dead space 조건을 추가할 수 있습니다. 이 값은 정확한 bloat 측정값이 아니라 계획 보조 지표로 봐야 합니다.

## Exit Code

스케줄러와 모니터링은 프로세스 exit code를 기준으로 판단할 수 있습니다.

| Code | 의미 |
| ---: | --- |
| `0` | 성공, 또는 테이블 단위 SKIP만 존재 |
| `1` | 대상 DB 목록 조회 실패 |
| `2` | 다른 실행이 advisory lock을 이미 보유 |
| `3` | control DB 접속 실패 |
| `4` | 하나 이상의 테이블 작업 실패 |
| `5` | graceful 또는 immediate stop으로 실행 중단 |
| `6` | standby 외 사유로 하나 이상의 DB가 SKIP |
| `7` | JSON 리포트 저장 실패, 그리고 `run.json_fail_on_error` 활성화 |
| `8` | 반복 SKIP 이력이 `run.skip_history_threshold`에 도달 |

precheck 충돌, lock timeout 같은 테이블 단위 SKIP은 리포트와 알림에 기록됩니다. 단발성 테이블 SKIP은 기본적으로 프로세스를 실패 처리하지 않지만, 반복 SKIP 정책이 활성화되어 있고 threshold에 도달하면 exit code `8`로 실패 처리할 수 있습니다.

## 결과 해석

현재 action 결과의 표준 상태는 다음 세 가지입니다.

- `OK`: 유지보수 작업 완료
- `SKIP`: stop signal, precheck, lock timeout 등 의도적으로 연기 가능한 조건으로 건너뜀
- `FAIL`: skip으로 분류되지 않은 실패

호환성을 위해 기존 `ok`, `skipped` 플래그도 유지하지만, 운영 판단의 기준 필드는 `status`입니다.

## 반복 SKIP 이력

`run.skip_history_enabled: true`이면 실행기는 테이블 단위 SKIP 이력을 `run.skip_history_path`에 저장합니다.

이력 key는 다음 조합입니다.

```text
database | schema.table | action | skip reason
```

카운트가 `run.skip_history_threshold`에 도달하면 run summary에 `skip_history.alerts`가 기록됩니다. `run.skip_history_fail_on_threshold: true`이면 프로세스는 exit code `8`로 종료됩니다.

`run.skip_history_reset_on_ok: true`이면 같은 DB/테이블/action의 이후 성공 결과가 이전 SKIP 이력을 초기화합니다.

## JSON 리포트

`run.json_auto_save: true`이면 `--json-out`을 따로 지정하지 않는 한 리포트가 `run.json_out_dir` 아래에 저장됩니다.

JSON 리포트는 다음 용도로 유용합니다.

- 감사 이력
- action 단위 장애 분석
- lock 또는 timeout 진단
- 도구 외부에서의 추세 분석

JSON 저장에 실패하면 현재 버전은 `json_save_failed`를 기록합니다. `run.json_fail_on_error: true`이면 프로세스는 exit code `7`로 종료됩니다.

## 메트릭

메트릭 출력은 best-effort 방식입니다. 메트릭 저장이나 전송 실패가 실행 자체를 실패시키지는 않습니다.

지원 출력은 다음과 같습니다.

- Prometheus node_exporter textfile collector
- StatsD UDP gauge metric

대표 메트릭 항목은 다음과 같습니다.

- 계획된 action 수
- `OK` / `SKIP` / `FAIL` 수
- SKIP된 DB 수
- JSON 저장 실패 여부
- 반복 SKIP alert 수
- 최종 exit code

## 알림

Slack과 Telegram 알림 실패는 실행 실패로 처리하지 않습니다. 실패 내용은 warning log로 남습니다.

SQL 텍스트가 메시지를 과도하게 길게 만들 수 있으므로, 디버깅 목적이 아니면 `run.notify_include_sql: false`를 유지하는 것이 좋습니다.

## 중지 처리

- `Ctrl+C` 1회: graceful stop. 새 작업 예약을 중단하고 진행 중인 작업은 끝내도록 둡니다.
- `Ctrl+C` 2회 또는 `SIGTERM`: immediate stop. 활성 worker connection 취소와 종료를 시도합니다.

## Cron 예시

```cron
0 2 * * * /usr/bin/python3 /opt/pg_vacman/pg_vacman.py \
  --config /opt/pg_vacman/config.local.yaml \
  --apply >> /var/log/pg_vacman/apply.log 2>&1
```

권장 운영 방식은 다음과 같습니다.

- 절대 경로를 사용합니다.
- stdout과 stderr를 모두 보관합니다.
- non-zero exit code를 모니터링합니다.
- 로그를 rotate합니다.
- JSON 리포트를 보관합니다.

## 장애 분석

`exit=2`

다른 실행이 활성 상태이거나 이전 세션이 아직 연결되어 있을 수 있습니다. `pg_stat_activity`에서 `application_name`에 `pg_vacman:control_lock`이 포함된 세션을 확인합니다.

`exit=3`

control connection 실패입니다. host, port, DB명, role, password, `pg_hba.conf`, 네트워크 접근성, TLS 요구사항을 확인합니다.

`exit=4`

하나 이상의 테이블 action이 실패했습니다. JSON 리포트에서 `FAIL` action과 PostgreSQL error detail을 확인합니다.

`exit=5`

signal에 의해 실행이 중단되었습니다. 계획된 중지인지, 외부 스케줄러 timeout인지 확인합니다.

`exit=6`

하나 이상의 DB가 standby 외 사유로 SKIP되었습니다. 대표 원인은 candidate discovery 실패 또는 global action limit 소진입니다.

`exit=7`

JSON 리포트 저장 실패입니다. 리포트 디렉터리 권한과 남은 디스크 공간을 확인합니다.

`exit=8`

반복 테이블 SKIP 이력이 threshold에 도달했습니다. run report의 `skip_history.alerts`와 history 파일을 확인합니다.

## 보안

- 실제 password, webhook URL, bot token을 Git에 커밋하지 않습니다.
- `config.local.yaml` 또는 환경별 설정 파일을 사용합니다.
- 대상 테이블 유지보수에 필요한 최소 권한을 가진 전용 PostgreSQL role을 사용합니다.
- 테이블명이나 운영 메타데이터가 민감할 수 있으므로 JSON 리포트 접근 권한을 제한합니다.
