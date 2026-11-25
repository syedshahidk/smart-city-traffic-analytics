from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any

from stream_processing.traffic_stream import (
    DEFAULT_CHECKPOINT_BASE,
    build_aggregates,
    checkpoint_path,
    hdfs_checkpoint_path,
    hdfs_metrics_path,
    parse_args,
    selected_datasets,
    watermark_advance_microseconds,
)
from stream_processing.replay_ledger import HdfsReplayVerifier


class CheckpointVersionTests(unittest.TestCase):
    def test_default_local_checkpoint_uses_dedup_version(self) -> None:
        self.assertEqual(DEFAULT_CHECKPOINT_BASE.name, "traffic_stream_deduplicated_v2")
        self.assertEqual(
            checkpoint_path(DEFAULT_CHECKPOINT_BASE, "metr-la", "postgres"),
            (DEFAULT_CHECKPOINT_BASE / "metr-la" / "postgres").resolve(),
        )
        self.assertNotEqual(
            checkpoint_path(DEFAULT_CHECKPOINT_BASE, "metr-la", "postgres"),
            (DEFAULT_CHECKPOINT_BASE.parent / "spark_docker" / "metr-la").resolve(),
        )
        self.assertNotEqual(
            checkpoint_path(DEFAULT_CHECKPOINT_BASE, "metr-la", "postgres"),
            checkpoint_path(DEFAULT_CHECKPOINT_BASE, "metr-la", "hdfs"),
        )
        self.assertNotEqual(
            checkpoint_path(DEFAULT_CHECKPOINT_BASE, "metr-la", "postgres"),
            checkpoint_path(DEFAULT_CHECKPOINT_BASE, "pems-bay", "postgres"),
        )
        ledger_checkpoint = checkpoint_path(DEFAULT_CHECKPOINT_BASE, "metr-la", "replay_ledger")
        self.assertNotEqual(ledger_checkpoint, checkpoint_path(DEFAULT_CHECKPOINT_BASE, "metr-la", "postgres"))
        self.assertNotEqual(ledger_checkpoint, checkpoint_path(DEFAULT_CHECKPOINT_BASE, "metr-la", "hdfs"))

    def test_compose_uses_new_dedup_checkpoint_without_reusing_old_path(self) -> None:
        compose = (Path(__file__).resolve().parents[1] / "docker-compose.spark.yml").read_text(encoding="utf-8")
        self.assertIn("/opt/project/storage/checkpoints/spark_deduplicated_v2", compose)
        self.assertNotIn("/opt/project/storage/checkpoints/spark_docker", compose)
        self.assertNotIn("spark_deduplicated_v1", compose)
        self.assertIn('"127.0.0.1:${SPARK_READINESS_PORT:-8081}:8081"', compose)
        self.assertIn("/health/ready", compose)
        self.assertIn(
            "replay_ledger",
            (Path(__file__).resolve().parents[1] / "stream_processing" / "traffic_stream.py").read_text(encoding="utf-8"),
        )

    def test_new_checkpoint_defaults_to_latest_offsets(self) -> None:
        args = parse_args([])
        self.assertEqual(args.starting_offsets, "latest")
        self.assertEqual(args.checkpoint_dir, DEFAULT_CHECKPOINT_BASE)

    def test_hdfs_checkpoint_uses_dedup_version(self) -> None:
        self.assertEqual(
            hdfs_checkpoint_path("pems-bay"),
            "hdfs://namenode:8020/traffic/system/checkpoints/"
            "streaming-metrics-deduplicated-v2/pems-bay/hdfs",
        )
        self.assertEqual(
            hdfs_metrics_path("metr-la"),
            "hdfs://namenode:8020/traffic/streaming/metrics-deduplicated-v2/metr-la",
        )

    def test_both_selects_independent_queries_and_replay_control_advances_trailing_window(self) -> None:
        self.assertEqual(selected_datasets("both"), ["metr-la", "pems-bay"])
        self.assertEqual(watermark_advance_microseconds("10 minutes"), 901_000_000)
        with self.assertRaises(ValueError):
            watermark_advance_microseconds("not a duration")
        with self.assertRaisesRegex(ValueError, "one dataset"):
            build_aggregates(None, threshold=20.0, dataset="both")


try:
    from pyspark.sql import SparkSession
except ImportError as exc:
    SparkSession = None  # type: ignore[assignment,misc]
    _SPARK_IMPORT_ERROR = exc
else:
    _SPARK_IMPORT_ERROR = None


@unittest.skipIf(SparkSession is None, f"PySpark unavailable: {_SPARK_IMPORT_ERROR}")
class SnapshotDeduplicationStreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        assert SparkSession is not None
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("traffic-snapshot-dedup-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "spark", None) is not None:
            cls.spark.stop()

    def _start_query(
        self,
        dataset: str = "metr-la",
        *,
        parquet: bool = False,
        root: Path | None = None,
    ) -> dict[str, Any]:
        temporary = tempfile.TemporaryDirectory(prefix="traffic-dedup-test-") if root is None else None
        root = Path(temporary.name) if temporary is not None else root
        assert root is not None
        input_dir = root / "input"
        input_dir.mkdir(exist_ok=True)
        outputs: list[list[dict[str, Any]]] = []
        raw_events = (
            self.spark.readStream.format("text")
            .option("maxFilesPerTrigger", 1)
            .load(str(input_dir))
        )
        aggregates = build_aggregates(
            raw_events, threshold=20.0, dataset=dataset, watermark_delay="10 minutes"
        )

        def collect_batch(batch_df: Any, _batch_id: int) -> None:
            outputs.append([row.asDict(recursive=True) for row in batch_df.collect()])

        writer = aggregates.writeStream.outputMode("append")
        if parquet:
            query = (
                writer.format("parquet")
                .option("path", str(root / "output"))
                .option("checkpointLocation", str(root / "checkpoint"))
                .start()
            )
        else:
            query = (
                writer.foreachBatch(collect_batch)
                .option("checkpointLocation", str(root / "checkpoint"))
                .start()
            )
        self.addCleanup(query.stop)
        if temporary is not None:
            self.addCleanup(temporary.cleanup)
        return {
            "input": input_dir,
            "outputs": outputs,
            "query": query,
            "next_file": 0,
            "root": root,
            "dataset": dataset,
            "parquet": parquet,
        }

    def _write_batch(self, state: dict[str, Any], events: list[dict[str, Any]]) -> None:
        state["next_file"] += 1
        path = state["input"] / f"batch-{state['next_file']:03d}.json"
        path.write_text(
            "\n".join(json.dumps(event, separators=(",", ":")) for event in events),
            encoding="utf-8",
        )
        state["query"].processAllAvailable()

    def _finalize_target_window(self, state: dict[str, Any]) -> None:
        dataset = state["dataset"]
        self._write_batch(
            state, [self._snapshot(dataset, "2024-01-01T00:20:00Z", [25.0])]
        )
        self._write_batch(
            state, [self._snapshot(dataset, "2024-01-01T00:25:00Z", [25.0])]
        )

    @staticmethod
    def _snapshot(
        dataset: str, timestamp: str, speeds: list[float | None]
    ) -> dict[str, Any]:
        return {
            "event_type": "traffic_snapshot",
            "dataset": dataset,
            "timestamp": timestamp,
            "sensor_count": len(speeds),
            "readings": [
                {"sensor_id": f"{dataset}-sensor-{index}", "speed": speed}
                for index, speed in enumerate(speeds)
            ],
        }

    @staticmethod
    def _latest(
        outputs: list[list[dict[str, Any]]], dataset: str, start: datetime
    ) -> dict[str, Any]:
        matches = [
            row
            for batch in outputs
            for row in batch
            if row["dataset"] == dataset and row["window_start"] == start
        ]
        if not matches:
            raise AssertionError(f"No aggregate for {dataset} at {start}")
        return matches[-1]

    def test_identical_snapshots_in_one_microbatch_count_once(self) -> None:
        state = self._start_query()
        speeds = [10.0, 15.0] + [62.5] * 205
        event = self._snapshot("metr-la", "2024-01-01T00:00:00Z", speeds)
        self._write_batch(state, [event, event, event])
        self._finalize_target_window(state)
        row = self._latest(state["outputs"], "metr-la", datetime(2024, 1, 1))
        self.assertEqual(row["total_sensor_readings"], 207)
        self.assertEqual(row["congested_sensor_count"], 2)
        self.assertAlmostEqual(row["congestion_percentage"], 200 / 207)

    def test_identical_snapshots_across_microbatches_count_once(self) -> None:
        state = self._start_query()
        speeds = [68.4] * 325
        event = self._snapshot("pems-bay", "2024-01-01T00:00:00Z", speeds)
        self._write_batch(state, [event])
        self._write_batch(state, [event])
        self._finalize_target_window(state)
        row = self._latest(state["outputs"], "pems-bay", datetime(2024, 1, 1))
        self.assertEqual(row["total_sensor_readings"], 325)
        self.assertEqual(row["congested_sensor_count"], 0)

    def test_dataset_filtering_precedes_independent_year_separated_watermarks(self) -> None:
        metr_state = self._start_query("metr-la")
        pems_state = self._start_query("pems-bay")
        mixed_batches = [
            [self._snapshot("pems-bay", "2017-01-01T00:00:00Z", [5.0, 40.0, 50.0])],
            [self._snapshot("metr-la", "2012-03-01T00:00:00Z", [10.0, 30.0])],
            [self._snapshot("pems-bay", "2017-01-01T00:20:00Z", [25.0])],
            [self._snapshot("metr-la", "2012-03-01T00:20:00Z", [25.0])],
            [self._snapshot("pems-bay", "2017-01-01T00:25:00Z", [25.0])],
            [self._snapshot("metr-la", "2012-03-01T00:25:00Z", [25.0])],
        ]
        # Each independently checkpointed query sees the shared topic order;
        # the unrelated 2017 events must not watermark away METR-LA's 2012 data.
        for batch in mixed_batches:
            self._write_batch(metr_state, batch)
            self._write_batch(pems_state, batch)
        metr_row = self._latest(metr_state["outputs"], "metr-la", datetime(2012, 3, 1))
        pems_row = self._latest(pems_state["outputs"], "pems-bay", datetime(2017, 1, 1))
        self.assertEqual(metr_row["total_sensor_readings"], 2)
        self.assertEqual(pems_row["total_sensor_readings"], 3)
        self.assertEqual(metr_row["congested_sensor_count"], 1)
        self.assertEqual(pems_row["congested_sensor_count"], 1)

    def test_distinct_timestamps_in_one_window_both_contribute(self) -> None:
        state = self._start_query()
        first = self._snapshot("metr-la", "2024-01-01T00:00:00Z", [10.0, 30.0])
        second = self._snapshot("metr-la", "2024-01-01T00:02:00Z", [15.0, 35.0])
        self._write_batch(state, [first])
        self._write_batch(state, [second])
        self._finalize_target_window(state)
        row = self._latest(state["outputs"], "metr-la", datetime(2024, 1, 1))
        self.assertEqual(row["total_sensor_readings"], 4)
        self.assertEqual(row["congested_sensor_count"], 2)
        self.assertAlmostEqual(row["average_speed"], 22.5)

    def test_watermark_bounds_dedup_state_and_drops_late_replay(self) -> None:
        state = self._start_query()
        initial = self._snapshot("metr-la", "2024-01-01T00:00:00Z", [10.0, 30.0])
        self._write_batch(state, [initial])
        self._write_batch(state, [self._snapshot("metr-la", "2024-01-01T00:20:00Z", [25.0])])
        self._write_batch(state, [self._snapshot("metr-la", "2024-01-01T00:40:00Z", [25.0])])
        self._write_batch(state, [self._snapshot("metr-la", "2024-01-01T01:00:00Z", [25.0])])
        progress = state["query"].recentProgress
        watermarks = [
            p.get("eventTime", {}).get("watermark")
            for p in progress
            if p.get("eventTime", {}).get("watermark")
        ]
        self.assertTrue(any(value >= "2024-01-01T00:10:00.000Z" for value in watermarks))
        self.assertTrue(
            any(
                operator.get("numRowsRemoved", 0) > 0
                for p in progress
                for operator in p.get("stateOperators", [])
            ),
            "watermark should evict expired deduplication state",
        )
        self._write_batch(state, [initial])
        row = self._latest(state["outputs"], "metr-la", datetime(2024, 1, 1))
        self.assertEqual(row["total_sensor_readings"], 2)
        self.assertEqual(row["congested_sensor_count"], 1)

    def test_nonduplicate_counts_preserve_null_zero_and_threshold_behavior(self) -> None:
        state = self._start_query("pems-bay")
        event = self._snapshot("pems-bay", "2024-01-01T00:00:00Z", [0.0, None, 20.0, 25.0])
        self._write_batch(state, [event])
        self._finalize_target_window(state)
        row = self._latest(state["outputs"], "pems-bay", datetime(2024, 1, 1))
        self.assertEqual(row["total_sensor_readings"], 3)
        self.assertEqual(row["congested_sensor_count"], 1)
        self.assertAlmostEqual(row["average_speed"], 15.0)
        self.assertAlmostEqual(row["congestion_percentage"], 100 / 3)

    def test_replay_completion_finalizes_trailing_window_without_a_metric_row(self) -> None:
        state = self._start_query("metr-la")
        timestamp = "2024-02-01T00:00:00Z"
        self._write_batch(state, [self._snapshot("metr-la", timestamp, [10.0, 30.0])])
        marker = {
            "event_type": "replay_complete",
            "dataset": "metr-la",
            "replay_id": "replay-a",
            "last_event_timestamp": timestamp,
        }
        self._write_batch(state, [marker])
        self._write_batch(state, [marker])
        row = self._latest(state["outputs"], "metr-la", datetime(2024, 2, 1))
        self.assertEqual(row["total_sensor_readings"], 2)
        self.assertEqual(row["congested_sensor_count"], 1)
        self.assertEqual(
            sum(
                1
                for batch in state["outputs"]
                for result in batch
                if result["dataset"] == "metr-la"
                and result["window_start"] == datetime(2024, 2, 1)
            ),
            1,
            "repeated completion controls must not duplicate finalized output",
        )

    def test_replay_completion_does_not_duplicate_hdfs_parquet_rows(self) -> None:
        state = self._start_query("metr-la", parquet=True)
        timestamp = "2024-02-01T00:00:00Z"
        self._write_batch(state, [self._snapshot("metr-la", timestamp, [10.0, 30.0])])
        marker = {
            "event_type": "replay_complete",
            "dataset": "metr-la",
            "replay_id": "replay-a",
            "last_event_timestamp": timestamp,
        }
        self._write_batch(state, [marker])
        self._write_batch(state, [marker])
        output = self.spark.read.parquet(str(state["root"] / "output"))
        rows = output.where(output.dataset == "metr-la").collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].total_sensor_readings, 2)

    def test_hdfs_checkpoint_recovery_does_not_duplicate_committed_rows(self) -> None:
        with tempfile.TemporaryDirectory(prefix="traffic-recovery-test-") as temporary:
            root = Path(temporary)
            first = self._start_query("metr-la", parquet=True, root=root)
            timestamp = "2024-02-01T00:00:00Z"
            self._write_batch(first, [self._snapshot("metr-la", timestamp, [10.0, 30.0])])
            marker = {
                "event_type": "replay_complete",
                "dataset": "metr-la",
                "replay_id": "replay-recovery",
                "last_event_timestamp": timestamp,
            }
            self._write_batch(first, [marker])
            first["query"].stop()
            before_restart = self.spark.read.parquet(str(root / "output")).count()

            resumed = self._start_query("metr-la", parquet=True, root=root)
            resumed["query"].processAllAvailable()
            after_restart = self.spark.read.parquet(str(root / "output")).count()
            resumed["query"].stop()
            self.assertEqual(before_restart, 1)
            self.assertEqual(after_restart, before_restart)

    def test_hdfs_replay_verifier_requires_checkpoint_and_sink_commit_evidence(self) -> None:
        state = self._start_query("metr-la", parquet=True)
        timestamp = "2024-02-01T00:00:00Z"
        self._write_batch(state, [self._snapshot("metr-la", timestamp, [10.0, 30.0])])
        self._write_batch(state, [{
            "event_type": "replay_complete",
            "dataset": "metr-la",
            "replay_id": "d76a66f1-922c-4b15-9f84-b89c18e331a2",
            "last_event_timestamp": timestamp,
        }])
        state["query"].stop()

        checkpoint = state["root"] / "checkpoint"
        offsets = checkpoint / "offsets"
        for offset_log in offsets.iterdir():
            if offset_log.is_file() and offset_log.name.isdigit():
                offset_log.write_text(
                    'v1\n{"batchWatermarkMs":0,"batchTimestampMs":1}\n'
                    '{"traffic-events":{"0":9}}',
                    encoding="utf-8",
                )

        class Cursor:
            rowcount = 1

            def __enter__(self) -> Cursor:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def execute(self, _sql: str, _params: tuple[object, ...]) -> None:
                return None

        class Connection:
            def __enter__(self) -> Connection:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def cursor(self) -> Cursor:
                return Cursor()

            def close(self) -> None:
                return None

        replay = {
            "replay_id": "d76a66f1-922c-4b15-9f84-b89c18e331a2",
            "dataset": "metr-la",
            "expected_final_window_start": datetime(2024, 2, 1),
            "expected_final_window_end": datetime(2024, 2, 1, 0, 5),
            "expected_average_speed": 20.0,
            "expected_total_sensor_readings": 2,
            "expected_congested_sensor_count": 1,
            "expected_congestion_percentage": 50.0,
            "marker_topic": "traffic-events",
            "marker_partition": 0,
            "marker_offset": 8,
            "marker_status": "observed",
            "hdfs_status": "pending",
        }
        verifier = HdfsReplayVerifier(
            self.spark,
            Connection,
            {"metr-la": str(checkpoint)},
            {"metr-la": str(state["root"] / "output")},
        )
        batch_id = verifier.verify(replay)
        self.assertIsNotNone(batch_id)


if __name__ == "__main__":
    unittest.main()
