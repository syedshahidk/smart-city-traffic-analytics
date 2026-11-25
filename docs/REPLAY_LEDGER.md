# Durable replay completion ledger

The v2 Spark replay workflow uses `public.traffic_replay_runs` to track a
replay from registration through Kafka marker observation and independent
PostgreSQL and HDFS verification. The application does not create or migrate
this table. Apply `docs/migrations/20261124_traffic_replay_runs.sql` manually
as a controlled database change before enabling replay with this version.

The migration is additive. It does not change `traffic_metrics` or existing
rows. A rollback is destructive to replay audit history: stop replay writers,
export the ledger if it must be retained, then drop only
`public.traffic_replay_runs` in an approved maintenance operation.

For a controlled deployment, first review the SQL and obtain approval, then
apply it as the existing PostgreSQL writer/owner role (this command is
documented only; it has not been run here):

```powershell
Get-Content -Raw docs/migrations/20261124_traffic_replay_runs.sql |
  docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

After the migration succeeds, start the Spark service deliberately and wait
for its health check before launching the producer:

```powershell
docker compose -f docker-compose.spark.yml up -d spark-streaming
Invoke-RestMethod 'http://127.0.0.1:8081/health/ready?datasets=metr-la'
```

## Replay lifecycle

1. The producer waits until all three queries (PostgreSQL, HDFS, and the replay
   marker observer) are active for each selected dataset. On an empty topic,
   current-run Spark idle status after its initial Kafka offset poll can satisfy
   readiness; a stale checkpoint or a query still initializing cannot.
2. It scans the selected processed CSV rows, computes the final five-minute
   window and its expected aggregate, then registers a UUID before publishing.
   `--replay-id` accepts a stable UUID for an intentional retry; with `both`,
   deterministic per-dataset UUIDs are derived from it.
3. The producer publishes the completion marker and may post Kafka's
   acknowledged topic, partition, and offset as informational evidence. The
   Spark marker observer records the actual coordinates from its Kafka source;
   those observed coordinates are canonical and are never gated by producer
   acknowledgment. If the two coordinate sets differ, replay status reports
   `marker_ack_discrepancy` while preserving both records. A mismatch does not
   alter the existing PostgreSQL/HDFS completion checks.
4. PostgreSQL verification requires the final expected aggregate in the actual
   `foreachBatch` output, a matching persisted `traffic_metrics` row, and a
   checkpoint end offset strictly beyond the marker offset. The batch ID is
   recorded for recovery.
5. HDFS verification requires the HDFS checkpoint's committed batch, a matching
   Kafka end offset, the native `_spark_metadata` commit entry for that exact
   batch, and the expected final row among files named by that entry. A replay
   remains pending when this evidence is unavailable or has been compacted.
6. `overall_status` becomes `complete` only after both sinks and the marker are
   verified. A Kafka flush or producer acknowledgment alone is not completion.

## Checkpoint paths

The new marker observer has its own checkpoint at
`<SPARK_CHECKPOINT_DIR>/<dataset>/replay_ledger`. Existing PostgreSQL v2,
HDFS v2, v1, and historical paths are not reused. The marker checkpoint is
non-stateful but durable so an acknowledged marker can be retried after a
process restart. Never remove or repoint it during a replay recovery.

## Running and checking a replay

After the migration is applied and the Spark service has been deliberately
started, confirm readiness for the selected dataset before running the
producer. A normal replay keeps the existing non-blocking default. To require
verified completion and fail on timeout, pass:

```powershell
python -m data_ingestion.producer --dataset metr-la --limit 0 `
  --wait-for-completion --completion-timeout 1800
```

Use `--dataset pems-bay` as needed. For both datasets, pass `--dataset both`.
The `--threshold` value must match the Spark service threshold. Check durable
status with:

```powershell
$replayId = 'paste-the-logged-replay-uuid-here'
Invoke-RestMethod "http://127.0.0.1:8081/replays/$replayId"
```

The producer logs each generated replay ID. For a deliberate retry, pass the
same ID using `--replay-id`; do not generate a new ID when reconciling an
interrupted run.

## Evidence retention limitation

The verifier reads only exact Spark batch offset/commit logs and the native
file-sink metadata entry for that same batch. If Spark has compacted or removed
the per-batch metadata before reconciliation, the HDFS result remains pending;
it is never inferred from directory contents. Production deployments that
require recovery after long outages should add a durable, per-replay HDFS
manifest or another transactionally linked sink receipt before enabling that
recovery guarantee.
