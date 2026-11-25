"""Durable replay registration and fail-closed sink reconciliation helpers.

The SQL migration is deliberately external to application startup. This module
never creates the ledger table and all database values are bound parameters.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Iterable
from urllib.parse import urlsplit
import uuid


DATASETS = frozenset(("metr-la", "pems-bay"))
LOGGER = logging.getLogger(__name__)
REGISTER_SQL = """
    INSERT INTO public.traffic_replay_runs (
        replay_id, dataset, expected_last_source_timestamp,
        expected_final_window_start, expected_final_window_end,
        expected_average_speed, expected_total_sensor_readings,
        expected_congested_sensor_count, expected_congestion_percentage
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (replay_id) DO NOTHING
"""
GET_REPLAY_SQL = """
    SELECT replay_id, dataset, expected_last_source_timestamp,
           expected_final_window_start, expected_final_window_end,
           expected_average_speed, expected_total_sensor_readings,
           expected_congested_sensor_count, expected_congestion_percentage,
           marker_topic, marker_partition, marker_offset, marker_status,
           producer_ack_topic, producer_ack_partition, producer_ack_offset,
           producer_acknowledged_at,
           postgres_status, postgres_batch_id, postgres_verified_at,
           hdfs_status, hdfs_batch_id, hdfs_verified_at, overall_status,
           status_reason, created_at, updated_at
    FROM public.traffic_replay_runs WHERE replay_id = %s
"""
MARKER_ACK_SQL = """
    UPDATE public.traffic_replay_runs
       SET producer_ack_topic = COALESCE(producer_ack_topic, %s),
           producer_ack_partition = COALESCE(producer_ack_partition, %s),
           producer_ack_offset = COALESCE(producer_ack_offset, %s),
           producer_acknowledged_at = COALESCE(
               producer_acknowledged_at, CURRENT_TIMESTAMP
           ),
           updated_at = CASE WHEN producer_ack_topic IS NULL
                             THEN CURRENT_TIMESTAMP ELSE updated_at END
     WHERE replay_id = %s
       AND (producer_ack_topic IS NULL
            OR (producer_ack_topic = %s AND producer_ack_partition = %s
                AND producer_ack_offset = %s))
"""
MARKER_ACK_COORDINATES_SQL = """
    SELECT producer_ack_topic, producer_ack_partition, producer_ack_offset
      FROM public.traffic_replay_runs
     WHERE replay_id = %s
"""
POSTGRES_VERIFY_SQL = """
    UPDATE public.traffic_replay_runs AS r
       SET postgres_status = 'verified', postgres_batch_id = %s,
           postgres_verified_at = CURRENT_TIMESTAMP,
           overall_status = CASE WHEN r.hdfs_status = 'verified'
                                      AND r.marker_status = 'observed'
                                 THEN 'complete' ELSE r.overall_status END,
           updated_at = CURRENT_TIMESTAMP, status_reason = NULL
     WHERE r.replay_id = %s AND r.dataset = %s
       AND r.expected_final_window_start = %s
       AND r.expected_final_window_end = %s
       AND r.marker_status = 'observed' AND r.postgres_status = 'pending'
       AND EXISTS (
           SELECT 1 FROM public.traffic_metrics AS m
            WHERE m.dataset = r.dataset
              AND m.window_start = r.expected_final_window_start
              AND m.window_end = r.expected_final_window_end
              AND (r.expected_total_sensor_readings IS NULL OR
                   m.total_sensor_readings = r.expected_total_sensor_readings)
              AND (r.expected_congested_sensor_count IS NULL OR
                   m.congested_sensor_count = r.expected_congested_sensor_count)
              AND (r.expected_average_speed IS NULL OR
                   abs(m.average_speed - r.expected_average_speed) <= 1e-6)
              AND (r.expected_congestion_percentage IS NULL OR
                   abs(m.congestion_percentage - r.expected_congestion_percentage) <= 1e-6)
       )
"""
HDFS_VERIFY_SQL = """
    UPDATE public.traffic_replay_runs
       SET hdfs_status = 'verified', hdfs_batch_id = %s,
           hdfs_verified_at = CURRENT_TIMESTAMP,
           overall_status = CASE WHEN postgres_status = 'verified'
                                      AND marker_status = 'observed'
                                 THEN 'complete' ELSE overall_status END,
           updated_at = CURRENT_TIMESTAMP, status_reason = NULL
     WHERE replay_id = %s AND marker_status = 'observed' AND hdfs_status = 'pending'
"""
PENDING_POSTGRES_SQL = """
    SELECT replay_id, dataset, expected_last_source_timestamp,
           expected_final_window_start, expected_final_window_end,
           expected_average_speed, expected_total_sensor_readings,
           expected_congested_sensor_count, expected_congestion_percentage,
           marker_topic, marker_partition, marker_offset, marker_status,
           producer_ack_topic, producer_ack_partition, producer_ack_offset,
           producer_acknowledged_at,
           postgres_status, postgres_batch_id, postgres_verified_at,
           hdfs_status, hdfs_batch_id, hdfs_verified_at, overall_status,
           status_reason, created_at, updated_at
      FROM public.traffic_replay_runs
     WHERE dataset = %s AND overall_status = 'pending'
"""
POSTGRES_CANDIDATE_SQL = """
    UPDATE public.traffic_replay_runs
       SET postgres_batch_id = %s, updated_at = CURRENT_TIMESTAMP
     WHERE replay_id = %s AND dataset = %s AND postgres_status = 'pending'
"""
OBSERVE_MARKER_SQL = """
    UPDATE public.traffic_replay_runs
       SET marker_topic = %s, marker_partition = %s, marker_offset = %s,
           marker_status = 'observed', updated_at = CURRENT_TIMESTAMP
     WHERE replay_id = %s AND dataset = %s
       AND expected_last_source_timestamp = %s
       AND (marker_status = 'pending'
            OR (marker_topic = %s AND marker_partition = %s AND marker_offset = %s))
"""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("replay timestamps must include a timezone")
    return value.astimezone(timezone.utc)


def _manifest_timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("replay timestamps must be ISO 8601") from exc
    if not isinstance(value, datetime):
        raise ValueError("replay timestamps must be ISO 8601 datetimes")
    return _utc(value)


def validate_replay_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize the pre-publish replay manifest."""
    replay_id = str(uuid.UUID(str(manifest["replay_id"])))
    dataset = str(manifest["dataset"])
    if dataset not in DATASETS:
        raise ValueError("unsupported replay dataset")
    last_source = _manifest_timestamp(manifest["expected_last_source_timestamp"])
    window_start = _manifest_timestamp(manifest["expected_final_window_start"])
    window_end = _manifest_timestamp(manifest["expected_final_window_end"])
    if (window_end - window_start).total_seconds() != 300:
        raise ValueError("expected final window must be exactly five minutes")
    if not window_start <= last_source < window_end:
        raise ValueError("last source timestamp must fall in the final window")
    readings = manifest.get("expected_total_sensor_readings")
    congested = manifest.get("expected_congested_sensor_count")
    if readings is not None:
        if isinstance(readings, bool) or int(readings) != readings or int(readings) < 0:
            raise ValueError("expected reading count must be a non-negative integer")
        readings = int(readings)
    if congested is not None:
        if isinstance(congested, bool) or int(congested) != congested or int(congested) < 0:
            raise ValueError("expected congested count must be a non-negative integer")
        congested = int(congested)
    if congested is not None and readings is not None and congested > readings:
        raise ValueError("expected congested count is invalid")
    average = manifest.get("expected_average_speed")
    percentage = manifest.get("expected_congestion_percentage")
    if average is not None:
        average = float(average)
        if not math.isfinite(average):
            raise ValueError("expected average speed must be finite")
    if percentage is not None:
        percentage = float(percentage)
        if not math.isfinite(percentage) or not 0 <= percentage <= 100:
            raise ValueError("expected congestion percentage must be between 0 and 100")
    return {
        "replay_id": replay_id,
        "dataset": dataset,
        "expected_last_source_timestamp": last_source,
        "expected_final_window_start": window_start,
        "expected_final_window_end": window_end,
        "expected_average_speed": average,
        "expected_total_sensor_readings": readings,
        "expected_congested_sensor_count": congested,
        "expected_congestion_percentage": percentage,
    }


def register_replay(connection: Any, manifest: dict[str, Any]) -> dict[str, Any]:
    """Insert a replay before Kafka publication; idempotent for crash recovery."""
    value = validate_replay_manifest(manifest)
    params = tuple(value[key] for key in (
        "replay_id", "dataset", "expected_last_source_timestamp",
        "expected_final_window_start", "expected_final_window_end",
        "expected_average_speed", "expected_total_sensor_readings",
        "expected_congested_sensor_count", "expected_congestion_percentage",
    ))
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(REGISTER_SQL, params)
            cursor.execute(GET_REPLAY_SQL, (value["replay_id"],))
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("replay registration was not persisted")
            # Reusing an id with different source bounds or dataset is unsafe.
            if (str(row[0]) != value["replay_id"] or row[1] != value["dataset"]
                    or _utc(row[2]) != value["expected_last_source_timestamp"]
                    or _utc(row[3]) != value["expected_final_window_start"]
                    or _utc(row[4]) != value["expected_final_window_end"]
                    or row[5] != value["expected_average_speed"]
                    or row[6] != value["expected_total_sensor_readings"]
                    or row[7] != value["expected_congested_sensor_count"]
                    or row[8] != value["expected_congestion_percentage"]):
                raise ValueError("replay_id already exists with a different manifest")
    return get_replay(connection, value["replay_id"])


def get_replay(connection: Any, replay_id: str) -> dict[str, Any] | None:
    normalized = str(uuid.UUID(str(replay_id)))
    with connection.cursor() as cursor:
        cursor.execute(GET_REPLAY_SQL, (normalized,))
        row = cursor.fetchone()
        if row is None:
            return None
        names = [column[0] for column in cursor.description]
        replay = {
            name: (str(value) if name == "replay_id" else value)
            for name, value in zip(names, row)
        }
        replay["marker_ack_discrepancy"] = marker_ack_discrepancy(replay)
        return replay


def marker_ack_discrepancy(replay: dict[str, Any]) -> dict[str, Any] | None:
    """Describe differing producer and Spark coordinates without changing either."""
    if replay.get("marker_status") != "observed" or replay.get("producer_ack_topic") is None:
        return None

    producer_ack = {
        "topic": replay["producer_ack_topic"],
        "partition": replay["producer_ack_partition"],
        "offset": replay["producer_ack_offset"],
    }
    spark_observed = {
        "topic": replay["marker_topic"],
        "partition": replay["marker_partition"],
        "offset": replay["marker_offset"],
    }
    if producer_ack == spark_observed:
        return None
    return {
        "message": "producer-acknowledged marker coordinates differ from Spark-observed coordinates",
        "producer_ack": producer_ack,
        "spark_observed": spark_observed,
    }


def acknowledge_marker(connection: Any, replay_id: str, topic: str, partition: int, offset: int) -> bool:
    return acknowledge_marker_detailed(connection, replay_id, topic, partition, offset)[
        "status"
    ] == "acknowledged"


def acknowledge_marker_detailed(
    connection: Any, replay_id: str, topic: str, partition: int, offset: int
) -> dict[str, Any]:
    """Record the first producer ACK and describe conflicting retries."""
    if not topic or partition < 0 or offset < 0:
        raise ValueError("invalid Kafka marker position")
    normalized_replay_id = str(uuid.UUID(replay_id))
    attempted_coordinates = {
        "topic": topic,
        "partition": partition,
        "offset": offset,
    }
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(MARKER_ACK_SQL, (
                topic, partition, offset, normalized_replay_id,
                topic, partition, offset,
            ))
            if cursor.rowcount == 1:
                return {"status": "acknowledged", "replay_id": normalized_replay_id}

            cursor.execute(MARKER_ACK_COORDINATES_SQL, (normalized_replay_id,))
            accepted = cursor.fetchone()
            if accepted is None:
                return {
                    "status": "conflict",
                    "conflict_type": "replay_not_found",
                    "replay_id": normalized_replay_id,
                    "attempted_coordinates": attempted_coordinates,
                }

            accepted_coordinates = {
                "topic": accepted[0],
                "partition": accepted[1],
                "offset": accepted[2],
            }
            if accepted_coordinates == attempted_coordinates:
                return {"status": "acknowledged", "replay_id": normalized_replay_id}
            if accepted_coordinates["topic"] is None:
                return {
                    "status": "conflict",
                    "conflict_type": "producer_ack_not_recorded",
                    "replay_id": normalized_replay_id,
                    "attempted_coordinates": attempted_coordinates,
                }

            diagnostic = {
                "event": "producer_marker_ack_conflict",
                "replay_id": normalized_replay_id,
                "accepted_coordinates": accepted_coordinates,
                "attempted_coordinates": attempted_coordinates,
            }
            LOGGER.warning(
                "%s",
                json.dumps(diagnostic, separators=(",", ":"), sort_keys=True),
                extra={
                    "event_name": "producer_marker_ack_conflict",
                    "replay_id": normalized_replay_id,
                    "accepted_coordinates": accepted_coordinates,
                    "attempted_coordinates": attempted_coordinates,
                },
            )
            return {
                "status": "conflict",
                "conflict_type": "producer_ack_coordinates_mismatch",
                "replay_id": normalized_replay_id,
                "accepted_coordinates": accepted_coordinates,
                "attempted_coordinates": attempted_coordinates,
            }


def verify_postgres_final_window(
    connection: Any,
    replay: dict[str, Any],
    batch_id: int,
    batch_rows: Iterable[Any],
    offset_log_path: Path,
) -> bool:
    """Verify this batch emitted the expected window after consuming its marker."""
    expected = replay
    batch_match = False
    for row in batch_rows:
        values = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        if (values.get("dataset") == expected["dataset"]
                and _utc(values.get("window_start")) == _utc(expected["expected_final_window_start"])
                and _utc(values.get("window_end")) == _utc(expected["expected_final_window_end"])
                and (expected["expected_total_sensor_readings"] is None or int(values["total_sensor_readings"]) == int(expected["expected_total_sensor_readings"]))
                and (expected["expected_congested_sensor_count"] is None or int(values["congested_sensor_count"]) == int(expected["expected_congested_sensor_count"]))
                and (expected["expected_average_speed"] is None or abs(float(values["average_speed"]) - float(expected["expected_average_speed"])) <= 1e-6)
                and (expected["expected_congestion_percentage"] is None or abs(float(values["congestion_percentage"]) - float(expected["expected_congestion_percentage"])) <= 1e-6)):
            batch_match = True
            break
    if not batch_match:
        return False
    with connection.cursor() as cursor:
        cursor.execute(POSTGRES_CANDIDATE_SQL, (
            batch_id, expected["replay_id"], expected["dataset"],
        ))
        if cursor.rowcount != 1:
            return False
        if expected["marker_status"] != "observed":
            return True  # candidate batch is durable; marker observer reconciles it later
    try:
        offsets = parse_offset_log(offset_log_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return True  # fail closed while preserving the candidate for recovery
    if not marker_consumed(
        offsets, expected["marker_topic"], int(expected["marker_partition"]), int(expected["marker_offset"])
    ):
        return True
    with connection.cursor() as cursor:
        cursor.execute(POSTGRES_VERIFY_SQL, (
            batch_id, expected["replay_id"], expected["dataset"],
            expected["expected_final_window_start"], expected["expected_final_window_end"],
        ))
        return cursor.rowcount == 1


def pending_postgres_replays(connection: Any, dataset: str) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(PENDING_POSTGRES_SQL, (dataset,))
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]


def reconcile_postgres_candidates(connection: Any, dataset: str, checkpoint_dir: Path) -> int:
    """Finish candidate verifications after marker offsets arrive, including recovery."""
    verified = 0
    for replay in pending_postgres_replays(connection, dataset):
        batch_id = replay.get("postgres_batch_id")
        if batch_id is None or replay["marker_status"] != "observed" or replay["postgres_status"] != "pending":
            continue
        offset_log = checkpoint_dir / "offsets" / str(batch_id)
        try:
            offsets = parse_offset_log(offset_log.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not marker_consumed(
            offsets, replay["marker_topic"], int(replay["marker_partition"]), int(replay["marker_offset"])
        ):
            continue
        with connection.cursor() as cursor:
            cursor.execute(POSTGRES_VERIFY_SQL, (
                batch_id, replay["replay_id"], replay["dataset"],
                replay["expected_final_window_start"], replay["expected_final_window_end"],
            ))
            verified += cursor.rowcount
    return verified


def verify_hdfs_commit(connection: Any, replay_id: str, batch_id: int) -> bool:
    """Persist HDFS evidence emitted by the exact checkpoint/file-sink batch."""
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(HDFS_VERIFY_SQL, (batch_id, str(uuid.UUID(replay_id))))
            return cursor.rowcount == 1


def observe_marker(connection: Any, event: dict[str, Any]) -> bool:
    """Persist the marker's actual Kafka coordinates from Spark's Kafka source."""
    replay_id = str(uuid.UUID(str(event["replay_id"])))
    dataset = str(event["dataset"])
    if dataset not in DATASETS:
        return False
    source_time = datetime.fromisoformat(str(event["last_event_timestamp"]).replace("Z", "+00:00"))
    if source_time.tzinfo is None:
        source_time = source_time.replace(tzinfo=timezone.utc)
    last_source = _utc(source_time)
    topic, partition, offset = str(event["topic"]), int(event["partition"]), int(event["offset"])
    with connection.cursor() as cursor:
        cursor.execute(OBSERVE_MARKER_SQL, (
            topic, partition, offset, replay_id, dataset, last_source,
            topic, partition, offset,
        ))
        return cursor.rowcount == 1


class PostgresReplayAPI:
    """Small internal HTTP facade; the caller supplies the existing DB connector."""

    def __init__(self, connection_factory: Any, reconcile: Any | None = None):
        self._connection_factory = connection_factory
        self._reconcile = reconcile

    def register(self, manifest: dict[str, Any]) -> dict[str, Any]:
        connection = self._connection_factory()
        try:
            return register_replay(connection, manifest)
        finally:
            connection.close()

    def get(self, replay_id: str) -> dict[str, Any] | None:
        connection = self._connection_factory()
        try:
            replay = get_replay(connection, replay_id)
        finally:
            connection.close()
        if (replay is not None and self._reconcile is not None
                and replay.get("overall_status") == "pending"):
            self._reconcile(replay)
            connection = self._connection_factory()
            try:
                replay = get_replay(connection, replay_id)
            finally:
                connection.close()
        return replay

    def acknowledge(
        self, replay_id: str, topic: str, partition: int, offset: int
    ) -> dict[str, Any]:
        connection = self._connection_factory()
        try:
            return acknowledge_marker_detailed(
                connection, replay_id, topic, partition, offset
            )
        finally:
            connection.close()


def parse_offset_log(text: str) -> dict[tuple[str, int], int]:
    """Parse Spark Kafka end offsets from a checkpoint offsets log; fail closed."""
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines or lines[0] != "v1":
        raise ValueError("unsupported Spark offset log version")
    candidates = []
    for line in lines[1:]:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value and all(isinstance(v, dict) for v in value.values()):
            candidates.append(value)
    if not candidates:
        raise ValueError("Kafka offsets are missing from Spark offset log")
    offsets: dict[tuple[str, int], int] = {}
    for topic, partitions in candidates[-1].items():
        for partition, offset in partitions.items():
            offsets[(str(topic), int(partition))] = int(offset)
    return offsets


def marker_consumed(offsets: dict[tuple[str, int], int], topic: str, partition: int, offset: int) -> bool:
    """Kafka end offsets are exclusive; marker offset must be strictly below end."""
    end = offsets.get((topic, partition))
    return end is not None and end > offset


def committed_files(metadata_text: str) -> list[str]:
    """Return only add actions in a native Spark FileStreamSink commit log."""
    lines = [line for line in metadata_text.splitlines() if line.strip()]
    if not lines or lines[0] != "v1":
        raise ValueError("unsupported Spark file-sink metadata version")
    files: list[str] = []
    for line in lines[1:]:
        action = json.loads(line)
        if action.get("action") == "add" and action.get("path"):
            files.append(str(action["path"]))
    return files


def resolve_committed_path(output_uri: str, path: str) -> str:
    """Resolve metadata's relative file path without allowing root traversal."""
    parsed = urlsplit(path)
    if parsed.scheme:
        root = urlsplit(output_uri)
        root_path = root.path.rstrip("/")
        if parsed.scheme != root.scheme or parsed.netloc != root.netloc or not parsed.path.startswith(root_path + "/"):
            raise ValueError("Spark sink metadata path is outside the configured output")
        return path
    relative = PurePosixPath(path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("unsafe path in Spark sink commit metadata")
    return output_uri.rstrip("/") + "/" + str(relative)


class HdfsReplayVerifier:
    """Prove HDFS completion from matching source offsets and FileStreamSink commits.

    It deliberately fails closed when Spark's exact per-batch metadata is no
    longer retained. Directory/file existence by itself is never evidence.
    """

    def __init__(self, spark: Any, connection_factory: Any, checkpoint_by_dataset: dict[str, str], output_by_dataset: dict[str, str]):
        self.spark = spark
        self.connection_factory = connection_factory
        self.checkpoint_by_dataset = checkpoint_by_dataset
        self.output_by_dataset = output_by_dataset

    def _fs(self, uri: str) -> tuple[Any, Any]:
        jvm = self.spark.sparkContext._jvm
        path = jvm.org.apache.hadoop.fs.Path(uri)
        return path.getFileSystem(self.spark.sparkContext._jsc.hadoopConfiguration()), path

    def _read_text(self, uri: str) -> str:
        jvm = self.spark.sparkContext._jvm
        fs, path = self._fs(uri)
        stream = fs.open(path)
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, "UTF-8"))
        lines: list[str] = []
        try:
            line = reader.readLine()
            while line is not None:
                lines.append(str(line))
                line = reader.readLine()
        finally:
            reader.close()
        return "\n".join(lines)

    def _batch_ids(self, directory: str) -> set[int]:
        fs, path = self._fs(directory)
        if not fs.exists(path):
            return set()
        values: set[int] = set()
        for status in fs.listStatus(path):
            name = str(status.getPath().getName())
            if name.isdigit():
                values.add(int(name))
        return values

    @staticmethod
    def _row_matches(row: Any, replay: dict[str, Any]) -> bool:
        start, end = row["window_start"], row["window_end"]
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        return (
            row["dataset"] == replay["dataset"]
            and _utc(start) == _utc(replay["expected_final_window_start"])
            and _utc(end) == _utc(replay["expected_final_window_end"])
            and int(row["total_sensor_readings"]) == int(replay["expected_total_sensor_readings"])
            and int(row["congested_sensor_count"]) == int(replay["expected_congested_sensor_count"])
            and abs(float(row["average_speed"]) - float(replay["expected_average_speed"])) <= 1e-6
            and abs(float(row["congestion_percentage"]) - float(replay["expected_congestion_percentage"])) <= 1e-6
        )

    def verify(self, replay: dict[str, Any]) -> int | None:
        """Find a committed batch after marker offset whose files contain exact row."""
        if replay.get("hdfs_status") == "verified":
            return replay.get("hdfs_batch_id")
        dataset = replay["dataset"]
        checkpoint = self.checkpoint_by_dataset[dataset].rstrip("/")
        output = self.output_by_dataset[dataset].rstrip("/")
        checkpoint_uri = checkpoint if checkpoint.startswith("hdfs://") else Path(checkpoint).as_uri()
        committed_batches = self._batch_ids(checkpoint_uri + "/commits")
        offset_batches = self._batch_ids(checkpoint_uri + "/offsets")
        sink_batches = self._batch_ids(output + "/_spark_metadata")
        for batch_id in sorted(committed_batches & offset_batches & sink_batches, reverse=True):
            try:
                offsets = parse_offset_log(self._read_text(f"{checkpoint_uri}/offsets/{batch_id}"))
                if not marker_consumed(
                    offsets, replay["marker_topic"], int(replay["marker_partition"]), int(replay["marker_offset"])
                ):
                    continue
                paths = committed_files(self._read_text(f"{output}/_spark_metadata/{batch_id}"))
                if not paths:
                    continue
                absolute_paths = [resolve_committed_path(output, value) for value in paths]
                rows = self.spark.read.option("basePath", output).parquet(*absolute_paths).collect()
                matching_rows = sum(1 for row in rows if self._row_matches(row, replay))
                if matching_rows != 1:
                    continue
                connection = self.connection_factory()
                try:
                    if verify_hdfs_commit(connection, replay["replay_id"], batch_id):
                        return batch_id
                finally:
                    connection.close()
            except Exception:
                # Missing/compacted/corrupt evidence stays pending, never complete.
                continue
        return None

    def reconcile_pending(self, dataset: str) -> None:
        connection = self.connection_factory()
        try:
            replays = [row for row in pending_postgres_replays(connection, dataset)
                       if row["marker_status"] == "observed" and row["hdfs_status"] == "pending"]
        finally:
            connection.close()
        for replay in replays:
            self.verify(replay)

