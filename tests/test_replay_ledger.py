from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
import uuid

from data_ingestion.producer import build_replay_manifest
from stream_processing.replay_ledger import (
    GET_REPLAY_SQL,
    MARKER_ACK_SQL,
    MARKER_ACK_COORDINATES_SQL,
    OBSERVE_MARKER_SQL,
    acknowledge_marker,
    acknowledge_marker_detailed,
    committed_files,
    get_replay,
    marker_ack_discrepancy,
    marker_consumed,
    observe_marker,
    parse_offset_log,
    resolve_committed_path,
    validate_replay_manifest,
)


class ReplayManifestTests(unittest.TestCase):
    def test_manifest_calculates_final_five_minute_window_and_expected_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "metr.csv"
            path.write_text(
                "timestamp,s1,s2\n"
                "2012-03-01T00:00:00,10,30\n"
                "2012-03-01T00:02:00,0,\n"
                "2012-03-01T00:02:00,0,\n"
                "2012-03-01T00:05:00,25,35\n",
                encoding="utf-8",
            )
            manifest = build_replay_manifest(
                "metr-la", path, limit=3, replay_id=str(uuid.uuid4()), threshold=20.0
            )
        self.assertEqual(manifest["expected_last_source_timestamp"], datetime(2012, 3, 1, 0, 2, tzinfo=timezone.utc))
        self.assertEqual(manifest["expected_final_window_start"], datetime(2012, 3, 1, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(manifest["expected_final_window_end"], datetime(2012, 3, 1, 0, 5, tzinfo=timezone.utc))
        self.assertEqual(manifest["expected_total_sensor_readings"], 3)
        self.assertEqual(manifest["expected_congested_sensor_count"], 2)
        self.assertAlmostEqual(manifest["expected_average_speed"], 40 / 3)

    def test_manifest_rejects_conflicting_duplicate_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "metr.csv"
            path.write_text(
                "timestamp,s1\n2012-03-01T00:00:00,10\n2012-03-01T00:00:00,11\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "conflicting snapshots"):
                build_replay_manifest("metr-la", path, limit=0, replay_id=str(uuid.uuid4()), threshold=20)

    def test_replay_manifest_requires_uuid_and_five_minute_aware_bounds(self) -> None:
        value = {
            "replay_id": str(uuid.uuid4()),
            "dataset": "pems-bay",
            "expected_last_source_timestamp": datetime(2017, 1, 1, tzinfo=timezone.utc),
            "expected_final_window_start": datetime(2017, 1, 1, tzinfo=timezone.utc),
            "expected_final_window_end": datetime(2017, 1, 1, 0, 5, tzinfo=timezone.utc),
            "expected_average_speed": 65.0,
            "expected_total_sensor_readings": 325,
            "expected_congested_sensor_count": 0,
            "expected_congestion_percentage": 0.0,
        }
        normalized = validate_replay_manifest(value)
        self.assertEqual(normalized["dataset"], "pems-bay")
        value["expected_final_window_end"] = datetime(2017, 1, 1, 0, 6, tzinfo=timezone.utc)
        with self.assertRaisesRegex(ValueError, "exactly five minutes"):
            validate_replay_manifest(value)

    def test_http_json_timestamp_strings_are_normalized_to_utc(self) -> None:
        start = "2017-01-01T00:00:00+00:00"
        end = "2017-01-01T00:05:00+00:00"
        value = validate_replay_manifest({
            "replay_id": str(uuid.uuid4()),
            "dataset": "pems-bay",
            "expected_last_source_timestamp": start,
            "expected_final_window_start": start,
            "expected_final_window_end": end,
            "expected_average_speed": 62.0,
            "expected_total_sensor_readings": 325,
            "expected_congested_sensor_count": 1,
            "expected_congestion_percentage": 100 / 325,
        })
        self.assertEqual(value["expected_final_window_start"], datetime(2017, 1, 1, tzinfo=timezone.utc))
        with self.assertRaisesRegex(ValueError, "timezone"):
            validate_replay_manifest({
                **value,
                "expected_last_source_timestamp": datetime(2017, 1, 1),
            })


class SparkCommitEvidenceTests(unittest.TestCase):
    def test_checkpoint_offsets_use_exclusive_end_offset_for_marker(self) -> None:
        raw = "v1\n{\"batchWatermarkMs\":0,\"batchTimestampMs\":1}\n{\"traffic-events\":{\"2\":41}}"
        offsets = parse_offset_log(raw)
        self.assertFalse(marker_consumed(offsets, "traffic-events", 2, 41))
        self.assertTrue(marker_consumed(offsets, "traffic-events", 2, 40))
        self.assertFalse(marker_consumed(offsets, "traffic-events", 1, 0))

    def test_only_add_actions_in_native_file_sink_metadata_are_output_files(self) -> None:
        raw = "v1\n" + "\n".join([
            json.dumps({"action": "add", "path": "dataset=metr-la/part-000.parquet"}),
            json.dumps({"action": "delete", "path": "dataset=metr-la/old.parquet"}),
        ])
        self.assertEqual(
            committed_files(raw), ["dataset=metr-la/part-000.parquet"]
        )
        self.assertEqual(
            resolve_committed_path("hdfs://namenode:8020/traffic/output", committed_files(raw)[0]),
            "hdfs://namenode:8020/traffic/output/dataset=metr-la/part-000.parquet",
        )

    def test_metadata_path_traversal_and_unsupported_logs_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsafe path"):
            resolve_committed_path("hdfs://namenode/output", "../outside.parquet")
        with self.assertRaisesRegex(ValueError, "version"):
            committed_files("v2\n{}")
        with self.assertRaisesRegex(ValueError, "offset log"):
            parse_offset_log("not-a-checkpoint-log")


class ParameterizedLedgerWritesTests(unittest.TestCase):
    class Cursor:
        def __init__(self) -> None:
            self.sql = ""
            self.params: tuple[object, ...] = ()
            self.rowcount = 1

        def __enter__(self) -> ParameterizedLedgerWritesTests.Cursor:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[object, ...]) -> None:
            self.sql, self.params = sql, params

    class Connection:
        def __init__(self) -> None:
            self.last_cursor = ParameterizedLedgerWritesTests.Cursor()

        def __enter__(self) -> ParameterizedLedgerWritesTests.Connection:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def cursor(self) -> ParameterizedLedgerWritesTests.Cursor:
            return self.last_cursor

    def test_marker_acknowledgment_uses_bound_values(self) -> None:
        connection = self.Connection()
        replay_id = str(uuid.uuid4())
        self.assertTrue(acknowledge_marker(connection, replay_id, "traffic-events", 1, 99))
        self.assertEqual(connection.last_cursor.sql, MARKER_ACK_SQL)
        self.assertEqual(connection.last_cursor.params, (
            "traffic-events", 1, 99, replay_id,
            "traffic-events", 1, 99,
        ))
        self.assertEqual(connection.last_cursor.sql.count("%s"), len(connection.last_cursor.params))

    def test_marker_observation_normalizes_naive_source_timestamp_as_utc(self) -> None:
        connection = self.Connection()
        self.assertTrue(observe_marker(connection, {
            "replay_id": str(uuid.uuid4()),
            "dataset": "metr-la",
            "last_event_timestamp": "2012-03-01T00:00:00",
            "topic": "traffic-events",
            "partition": 0,
            "offset": 7,
        }))
        self.assertEqual(connection.last_cursor.params[5], datetime(2012, 3, 1, tzinfo=timezone.utc))


class ReplayMarkerOrderingTests(unittest.TestCase):
    class Ledger:
        def __init__(self) -> None:
            self.replays: dict[str, dict[str, object]] = {}

        def register(self, replay_id: str, dataset: str = "metr-la") -> None:
            self.replays[replay_id] = {
                "replay_id": replay_id,
                "dataset": dataset,
                "expected_last_source_timestamp": datetime(2012, 3, 1, tzinfo=timezone.utc),
                "marker_topic": None,
                "marker_partition": None,
                "marker_offset": None,
                "marker_status": "pending",
                "producer_ack_topic": None,
                "producer_ack_partition": None,
                "producer_ack_offset": None,
            }

        class Cursor:
            def __init__(self, ledger: ReplayMarkerOrderingTests.Ledger) -> None:
                self.ledger = ledger
                self.rowcount = 0
                self.result: tuple[object, ...] | None = None

            def __enter__(self) -> ReplayMarkerOrderingTests.Ledger.Cursor:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def execute(self, sql: str, params: tuple[object, ...]) -> None:
                if sql == MARKER_ACK_SQL:
                    topic, partition, offset, replay_id, *_ = params
                    replay = self.ledger.replays.get(str(replay_id))
                    if replay is None:
                        self.rowcount = 0
                        return
                    previous = (
                        replay["producer_ack_topic"],
                        replay["producer_ack_partition"],
                        replay["producer_ack_offset"],
                    )
                    acknowledged = (topic, partition, offset)
                    if previous[0] is not None and previous != acknowledged:
                        self.rowcount = 0
                        return
                    replay["producer_ack_topic"] = topic
                    replay["producer_ack_partition"] = partition
                    replay["producer_ack_offset"] = offset
                    self.rowcount = 1
                    return

                if sql == MARKER_ACK_COORDINATES_SQL:
                    replay_id = str(params[0])
                    replay = self.ledger.replays.get(replay_id)
                    self.result = None if replay is None else (
                        replay["producer_ack_topic"],
                        replay["producer_ack_partition"],
                        replay["producer_ack_offset"],
                    )
                    return

                if sql == OBSERVE_MARKER_SQL:
                    (
                        topic, partition, offset, replay_id, dataset, source_time,
                        *_,
                    ) = params
                    replay = self.ledger.replays.get(str(replay_id))
                    if (replay is None or replay["dataset"] != dataset
                            or replay["expected_last_source_timestamp"] != source_time):
                        self.rowcount = 0
                        return
                    observed = (topic, partition, offset)
                    previous = (
                        replay["marker_topic"],
                        replay["marker_partition"],
                        replay["marker_offset"],
                    )
                    if replay["marker_status"] == "observed" and previous != observed:
                        self.rowcount = 0
                        return
                    replay["marker_topic"] = topic
                    replay["marker_partition"] = partition
                    replay["marker_offset"] = offset
                    replay["marker_status"] = "observed"
                    self.rowcount = 1
                    return

                raise AssertionError(f"unexpected ledger SQL: {sql}")

            def fetchone(self) -> tuple[object, ...] | None:
                return self.result

        class Connection:
            def __init__(self, ledger: ReplayMarkerOrderingTests.Ledger) -> None:
                self.ledger = ledger
                self.last_cursor = ReplayMarkerOrderingTests.Ledger.Cursor(ledger)

            def __enter__(self) -> ReplayMarkerOrderingTests.Ledger.Connection:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def cursor(self) -> ReplayMarkerOrderingTests.Ledger.Cursor:
                self.last_cursor = ReplayMarkerOrderingTests.Ledger.Cursor(self.ledger)
                return self.last_cursor

            def close(self) -> None:
                return None

    @staticmethod
    def _marker(replay_id: str, offset: int, dataset: str = "metr-la") -> dict[str, object]:
        return {
            "replay_id": replay_id,
            "dataset": dataset,
            "last_event_timestamp": "2012-03-01T00:00:00Z",
            "topic": "traffic-events",
            "partition": 2,
            "offset": offset,
        }

    @staticmethod
    def _connection(ledger: Ledger) -> Ledger.Connection:
        return ReplayMarkerOrderingTests.Ledger.Connection(ledger)

    def test_ack_before_observation_accepts_matching_coordinates(self) -> None:
        replay_id = str(uuid.uuid4())
        ledger = self.Ledger()
        ledger.register(replay_id)
        connection = self._connection(ledger)

        self.assertTrue(acknowledge_marker(connection, replay_id, "traffic-events", 2, 17))
        self.assertTrue(observe_marker(connection, self._marker(replay_id, 17)))
        replay = ledger.replays[replay_id]
        self.assertEqual(replay["marker_offset"], 17)
        self.assertIsNone(marker_ack_discrepancy(replay))

    def test_ack_before_observation_cannot_override_spark_coordinates(self) -> None:
        replay_id = str(uuid.uuid4())
        ledger = self.Ledger()
        ledger.register(replay_id)
        connection = self._connection(ledger)

        self.assertTrue(acknowledge_marker(connection, replay_id, "traffic-events", 2, 17))
        self.assertTrue(observe_marker(connection, self._marker(replay_id, 18)))
        replay = ledger.replays[replay_id]
        self.assertEqual(replay["marker_offset"], 18)
        self.assertEqual(
            marker_ack_discrepancy(replay),
            {
                "message": "producer-acknowledged marker coordinates differ from Spark-observed coordinates",
                "producer_ack": {"topic": "traffic-events", "partition": 2, "offset": 17},
                "spark_observed": {"topic": "traffic-events", "partition": 2, "offset": 18},
            },
        )

    def test_observation_before_matching_ack_is_idempotent(self) -> None:
        replay_id = str(uuid.uuid4())
        ledger = self.Ledger()
        ledger.register(replay_id)
        connection = self._connection(ledger)
        marker = self._marker(replay_id, 27)

        self.assertTrue(observe_marker(connection, marker))
        self.assertTrue(acknowledge_marker(connection, replay_id, "traffic-events", 2, 27))
        self.assertTrue(observe_marker(connection, marker))
        self.assertTrue(acknowledge_marker(connection, replay_id, "traffic-events", 2, 27))
        replay = ledger.replays[replay_id]
        self.assertEqual(replay["marker_offset"], 27)
        self.assertIsNone(marker_ack_discrepancy(replay))

    def test_observation_before_mismatching_ack_preserves_both_records(self) -> None:
        replay_id = str(uuid.uuid4())
        ledger = self.Ledger()
        ledger.register(replay_id)
        connection = self._connection(ledger)

        self.assertTrue(observe_marker(connection, self._marker(replay_id, 37)))
        self.assertTrue(acknowledge_marker(connection, replay_id, "traffic-events", 2, 38))
        replay = ledger.replays[replay_id]
        self.assertEqual(replay["marker_offset"], 37)
        self.assertEqual(replay["producer_ack_offset"], 38)
        self.assertIsNotNone(marker_ack_discrepancy(replay))

        with self.assertLogs("stream_processing.replay_ledger", level="WARNING") as captured:
            self.assertFalse(acknowledge_marker(connection, replay_id, "traffic-events", 2, 39))
        record = captured.records[0]
        self.assertEqual(record.event_name, "producer_marker_ack_conflict")
        self.assertEqual(record.replay_id, replay_id)
        self.assertEqual(json.loads(record.getMessage())["event"], "producer_marker_ack_conflict")
        self.assertEqual(
            record.accepted_coordinates,
            {"topic": "traffic-events", "partition": 2, "offset": 38},
        )
        self.assertEqual(
            record.attempted_coordinates,
            {"topic": "traffic-events", "partition": 2, "offset": 39},
        )
        self.assertFalse(observe_marker(connection, self._marker(replay_id, 39)))
        self.assertEqual(replay["marker_offset"], 37)
        self.assertEqual(replay["producer_ack_offset"], 38)

    def test_conflicting_ack_result_identifies_replay_and_coordinate_sets(self) -> None:
        replay_id = str(uuid.uuid4())
        ledger = self.Ledger()
        ledger.register(replay_id)
        connection = self._connection(ledger)
        self.assertTrue(acknowledge_marker(connection, replay_id, "traffic-events", 2, 40))

        with self.assertLogs("stream_processing.replay_ledger", level="WARNING"):
            result = acknowledge_marker_detailed(
                connection, replay_id, "traffic-events", 2, 41
            )
        self.assertEqual(result, {
            "status": "conflict",
            "conflict_type": "producer_ack_coordinates_mismatch",
            "replay_id": replay_id,
            "accepted_coordinates": {
                "topic": "traffic-events", "partition": 2, "offset": 40,
            },
            "attempted_coordinates": {
                "topic": "traffic-events", "partition": 2, "offset": 41,
            },
        })
        self.assertEqual(ledger.replays[replay_id]["producer_ack_offset"], 40)

    def test_replay_status_exposes_persisted_coordinate_discrepancy(self) -> None:
        replay_id = str(uuid.uuid4())
        values = {
            "replay_id": replay_id,
            "dataset": "metr-la",
            "expected_last_source_timestamp": datetime(2012, 3, 1, tzinfo=timezone.utc),
            "expected_final_window_start": datetime(2012, 3, 1, tzinfo=timezone.utc),
            "expected_final_window_end": datetime(2012, 3, 1, 0, 5, tzinfo=timezone.utc),
            "expected_average_speed": 40.0,
            "expected_total_sensor_readings": 2,
            "expected_congested_sensor_count": 1,
            "expected_congestion_percentage": 50.0,
            "marker_topic": "traffic-events",
            "marker_partition": 2,
            "marker_offset": 67,
            "marker_status": "observed",
            "producer_ack_topic": "traffic-events",
            "producer_ack_partition": 2,
            "producer_ack_offset": 68,
            "producer_acknowledged_at": datetime(2012, 3, 1, tzinfo=timezone.utc),
            "postgres_status": "pending",
            "postgres_batch_id": None,
            "postgres_verified_at": None,
            "hdfs_status": "pending",
            "hdfs_batch_id": None,
            "hdfs_verified_at": None,
            "overall_status": "pending",
            "status_reason": None,
            "created_at": datetime(2012, 3, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2012, 3, 1, tzinfo=timezone.utc),
        }

        class Cursor:
            description = [(name,) for name in values]

            def __enter__(self) -> Cursor:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def execute(self, sql: str, params: tuple[object, ...]) -> None:
                self.assert_query(sql, params)

            @staticmethod
            def assert_query(sql: str, params: tuple[object, ...]) -> None:
                if sql != GET_REPLAY_SQL or params != (replay_id,):
                    raise AssertionError("unexpected replay lookup")

            def fetchone(self) -> tuple[object, ...]:
                return tuple(values.values())

        class Connection:
            def cursor(self) -> Cursor:
                return Cursor()

        replay = get_replay(Connection(), replay_id)
        self.assertIsNotNone(replay)
        assert replay is not None
        self.assertEqual(replay["marker_offset"], 67)
        self.assertEqual(replay["producer_ack_offset"], 68)
        self.assertEqual(replay["marker_ack_discrepancy"]["spark_observed"]["offset"], 67)

    def test_acknowledgement_and_observation_are_independent_and_replay_scoped(self) -> None:
        self.assertNotIn("marker_status", MARKER_ACK_SQL)
        self.assertNotIn("marker_topic", MARKER_ACK_SQL)
        self.assertNotIn("producer_ack", OBSERVE_MARKER_SQL)

        first_id, second_id = str(uuid.uuid4()), str(uuid.uuid4())
        ledger = self.Ledger()
        ledger.register(first_id, "metr-la")
        ledger.register(second_id, "pems-bay")
        connection = self._connection(ledger)

        self.assertFalse(observe_marker(connection, self._marker(first_id, 47, "pems-bay")))
        self.assertTrue(observe_marker(connection, self._marker(second_id, 57, "pems-bay")))
        self.assertEqual(ledger.replays[first_id]["marker_status"], "pending")
        self.assertEqual(ledger.replays[second_id]["marker_offset"], 57)


if __name__ == "__main__":
    unittest.main()
