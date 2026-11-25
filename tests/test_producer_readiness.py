from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen
from urllib.request import Request

from data_ingestion import producer
from stream_processing.readiness import ReadinessState, SparkReady, start_readiness_server


class FakeQuery:
    def __init__(
        self,
        *,
        active: bool = True,
        progress: bool = True,
        run_id: str = "run-a",
        error: Exception | None = None,
    ):
        self.id = "query-a"
        self.runId = run_id
        self.isActive = active
        self.error = error
        self.lastProgress = self.make_progress(run_id) if progress else None
        self.status = {"message": "Initializing sources", "isDataAvailable": False, "isTriggerActive": False}

    def make_progress(self, run_id: str, *, source: str = "traffic-events",
                      sink: str = "ForeachBatchSink") -> dict[str, object]:
        return {
            "id": self.id,
            "runId": run_id,
            "batchId": 0,
            "sources": [{
                "description": f"KafkaV2[Subscribe[{source}]]",
                "endOffset": f'{{"{source}":{{"0":1}}}}',
            }],
            "sink": {"description": sink},
        }

    def exception(self) -> Exception | None:
        return self.error


class ReadinessCoordinationTests(unittest.TestCase):
    @staticmethod
    def _register(state: ReadinessState, dataset: str, sink: str, query: FakeQuery) -> None:
        state.register_query(
            dataset,
            sink,
            query,
            expected_source="traffic-events",
            expected_sink="ForeachBatchSink",
        )

    def test_both_datasets_require_both_sink_queries_to_initialize(self) -> None:
        state = ReadinessState({"metr-la", "pems-bay"}, "10 minutes", "generation-a")
        for dataset in ("metr-la", "pems-bay"):
            self._register(state, dataset, "postgres", FakeQuery())
            self._register(state, dataset, "hdfs", FakeQuery(progress=False))
            self._register(state, dataset, "replay_ledger", FakeQuery())
        status, payload = state.response({"metr-la", "pems-bay"})
        self.assertEqual(status, 503)
        self.assertEqual(payload["ready_datasets"], [])

        self._register(state, "metr-la", "hdfs", FakeQuery())
        status, payload = state.response({"metr-la", "pems-bay"})
        self.assertEqual(status, 503)
        self.assertEqual(payload["ready_datasets"], ["metr-la"])

        self._register(state, "pems-bay", "hdfs", FakeQuery())
        status, payload = state.response({"metr-la", "pems-bay"})
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["ready_datasets"], ["metr-la", "pems-bay"])

    def test_unavailable_query_removes_dataset_from_readiness(self) -> None:
        state = ReadinessState({"metr-la"}, "10 minutes", "generation-a")
        self._register(state, "metr-la", "postgres", FakeQuery())
        self._register(state, "metr-la", "hdfs", FakeQuery(active=False))
        self._register(state, "metr-la", "replay_ledger", FakeQuery())
        self.assertEqual(state.response({"metr-la"})[0], 503)

    def test_empty_kafka_topic_is_ready_after_current_run_polled_offsets(self) -> None:
        state = ReadinessState({"metr-la"}, "10 minutes", "generation-empty")
        for sink in ("postgres", "hdfs", "replay_ledger"):
            query = FakeQuery(progress=False)
            query.status = {"message": "Waiting for next trigger", "isDataAvailable": False, "isTriggerActive": False}
            self._register(state, "metr-la", sink, query)
        status, payload = state.response({"metr-la"})
        self.assertEqual(status, 200)
        self.assertEqual(payload["ready_datasets"], ["metr-la"])
        self.assertTrue(payload["diagnostics"]["metr-la"]["replay_ledger"]["initial_offsets_confirmed"])

    def test_query_failure_during_recovery_never_reports_ready(self) -> None:
        state = ReadinessState({"metr-la"}, "10 minutes", "generation-f")
        self._register(state, "metr-la", "postgres", FakeQuery())
        self._register(
            state, "metr-la", "hdfs", FakeQuery(active=False, error=RuntimeError("checkpoint error"))
        )
        self._register(state, "metr-la", "replay_ledger", FakeQuery())
        status, payload = state.response({"metr-la"})
        self.assertEqual(status, 503)
        self.assertEqual(payload["diagnostics"]["metr-la"]["hdfs"]["state"], "failed")
        self.assertIn("checkpoint error", payload["diagnostics"]["metr-la"]["hdfs"]["reason"])

    def test_stale_checkpoint_metadata_does_not_satisfy_current_run_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            checkpoint = Path(temporary)
            state = ReadinessState({"metr-la"}, "10 minutes", "generation-c")
            (checkpoint / "sources" / "0").mkdir(parents=True)
            (checkpoint / "sources" / "0" / "0").write_text("stale", encoding="utf-8")
            query = FakeQuery(progress=False)
            self._register(state, "metr-la", "postgres", query)
            self._register(state, "metr-la", "hdfs", FakeQuery())
            self._register(state, "metr-la", "replay_ledger", FakeQuery())
            status, payload = state.response({"metr-la"})
            self.assertEqual(status, 503)
            self.assertEqual(
                payload["diagnostics"]["metr-la"]["postgres"]["state"],
                "awaiting_progress",
            )

            query.lastProgress = query.make_progress(str(query.runId))
            self.assertEqual(state.response({"metr-la"})[0], 200)

    def test_progress_from_previous_run_does_not_satisfy_readiness(self) -> None:
        state = ReadinessState({"metr-la"}, "10 minutes", "generation-d")
        query = FakeQuery(run_id="run-current")
        query.lastProgress = query.make_progress("run-previous")
        self._register(state, "metr-la", "postgres", query)
        self._register(state, "metr-la", "hdfs", FakeQuery())
        self._register(state, "metr-la", "replay_ledger", FakeQuery())
        status, payload = state.response({"metr-la"})
        self.assertEqual(status, 503)
        self.assertEqual(
            payload["diagnostics"]["metr-la"]["postgres"]["state"],
            "awaiting_progress",
        )

    def test_wrong_source_or_sink_progress_does_not_satisfy_readiness(self) -> None:
        state = ReadinessState({"metr-la"}, "10 minutes", "generation-e")
        postgres = FakeQuery()
        postgres.lastProgress = postgres.make_progress("run-a", source="other-topic")
        hdfs = FakeQuery()
        hdfs.lastProgress = hdfs.make_progress("run-a", sink="FileSink[other-path]")
        state.register_query(
            "metr-la", "postgres", postgres,
            expected_source="traffic-events", expected_sink="ForeachBatchSink",
        )
        state.register_query(
            "metr-la", "hdfs", hdfs,
            expected_source="traffic-events", expected_sink="hdfs://expected/output",
        )
        self._register(state, "metr-la", "replay_ledger", FakeQuery())
        status, payload = state.response({"metr-la"})
        self.assertEqual(status, 503)
        self.assertEqual(payload["diagnostics"]["metr-la"]["postgres"]["state"], "source_mismatch")
        self.assertEqual(payload["diagnostics"]["metr-la"]["hdfs"]["state"], "sink_mismatch")

    def test_http_readiness_endpoint_reports_current_query_state(self) -> None:
        state = ReadinessState({"metr-la"}, "10 minutes", "generation-a")
        self._register(state, "metr-la", "postgres", FakeQuery())
        self._register(state, "metr-la", "hdfs", FakeQuery())
        self._register(state, "metr-la", "replay_ledger", FakeQuery())
        server, _thread = start_readiness_server(state, host="127.0.0.1", port=0)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_address[1]
        with urlopen(f"http://127.0.0.1:{port}/health/ready?datasets=metr-la") as response:
            payload = json.loads(response.read().decode("utf-8"))
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["generation"], "generation-a")

    def test_replay_registration_and_status_are_exposed_by_internal_service(self) -> None:
        class ReplayAPI:
            def __init__(self) -> None:
                self.replays: dict[str, dict[str, object]] = {}

            def register(self, value: dict[str, object]) -> dict[str, object]:
                replay_id = str(value["replay_id"])
                self.replays[replay_id] = {**value, "overall_status": "pending"}
                return self.replays[replay_id]

            def get(self, replay_id: str) -> dict[str, object] | None:
                return self.replays.get(replay_id)

            def acknowledge(
                self, replay_id: str, topic: str, partition: int, offset: int
            ) -> dict[str, object]:
                replay = self.replays.get(replay_id)
                if replay is None:
                    return {
                        "status": "conflict",
                        "conflict_type": "replay_not_found",
                        "replay_id": replay_id,
                    }
                accepted = (
                    replay.get("producer_ack_topic"),
                    replay.get("producer_ack_partition"),
                    replay.get("producer_ack_offset"),
                )
                attempted = (topic, partition, offset)
                if accepted[0] is not None and accepted != attempted:
                    return {
                        "status": "conflict",
                        "conflict_type": "producer_ack_coordinates_mismatch",
                        "replay_id": replay_id,
                        "accepted_coordinates": {
                            "topic": accepted[0],
                            "partition": accepted[1],
                            "offset": accepted[2],
                        },
                        "attempted_coordinates": {
                            "topic": topic,
                            "partition": partition,
                            "offset": offset,
                        },
                    }
                replay.update(
                    producer_ack_topic=topic,
                    producer_ack_partition=partition,
                    producer_ack_offset=offset,
                )
                return {"status": "acknowledged", "replay_id": replay_id}

        state = ReadinessState({"metr-la"}, "10 minutes", "generation-ledger")
        api = ReplayAPI()
        server, _thread = start_readiness_server(state, host="127.0.0.1", port=0, replay_api=api)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_address[1]}"
        manifest = {"replay_id": "replay-a", "dataset": "metr-la"}
        request = Request(
            url + "/replays", data=json.dumps(manifest).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urlopen(request) as response:
            self.assertEqual(response.status, 201)
        with urlopen(url + "/replays/replay-a") as response:
            status = json.loads(response.read().decode())
        self.assertEqual(status["overall_status"], "pending")
        marker_request = Request(
            url + "/replays/replay-a/marker",
            data=json.dumps({"topic": "traffic-events", "partition": 2, "offset": 13}).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urlopen(marker_request) as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(api.replays["replay-a"]["producer_ack_offset"], 13)

        conflicting_request = Request(
            url + "/replays/replay-a/marker",
            data=json.dumps({"topic": "traffic-events", "partition": 2, "offset": 14}).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with self.assertRaises(HTTPError) as error:
            urlopen(conflicting_request)
        self.assertEqual(error.exception.code, 409)
        payload = json.loads(error.exception.read().decode())
        self.assertEqual(payload["status"], "conflict")
        self.assertEqual(payload["conflict_type"], "producer_ack_coordinates_mismatch")
        self.assertEqual(payload["replay_id"], "replay-a")
        self.assertEqual(payload["accepted_coordinates"]["offset"], 13)
        self.assertEqual(payload["attempted_coordinates"]["offset"], 14)
        self.assertEqual(api.replays["replay-a"]["producer_ack_offset"], 13)

    def test_producer_waits_for_readiness_response(self) -> None:
        payload = {
            "status": "ready",
            "ready_datasets": ["metr-la", "pems-bay"],
            "watermark_delay": "10 minutes",
            "generation": "generation-b",
        }

        class Response:
            status = 200

            def __enter__(self) -> Response:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(payload).encode()

        with patch("data_ingestion.producer.urlopen", return_value=Response()) as open_url:
            ready = producer.wait_for_spark_ready(
                "http://spark/health/ready",
                {"metr-la", "pems-bay"},
                timeout=1,
            )
        self.assertEqual(ready.generation, "generation-b")
        self.assertIn("datasets=metr-la%2Cpems-bay", open_url.call_args.args[0])

    def test_replay_dataset_cannot_publish_before_its_dataset_is_ready(self) -> None:
        class KafkaSink:
            def __init__(self) -> None:
                self.events: list[dict[str, object]] = []

            def send(self, _topic: str, *, key: bytes, value: dict[str, object]) -> None:
                self.events.append({"key": key, "value": value})

        sink = KafkaSink()
        with self.assertRaisesRegex(RuntimeError, "not confirmed readiness"):
            producer.replay_dataset(
                sink,
                "metr-la",
                Path("unused.csv"),
                limit=1,
                delay=0,
                readiness=SparkReady(frozenset({"pems-bay"}), "10 minutes", "g"),
            )
        self.assertEqual(sink.events, [])

    def test_dataset_events_are_partition_keyed_for_ordered_completion(self) -> None:
        class KafkaSink:
            def __init__(self) -> None:
                self.events: list[tuple[bytes, dict[str, object]]] = []

            def send(self, _topic: str, *, key: bytes, value: dict[str, object]) -> None:
                self.events.append((key, value))

        with tempfile.TemporaryDirectory() as temporary:
            csv_path = Path(temporary) / "metr.csv"
            csv_path.write_text(
                "timestamp,s1\n2012-03-01T00:00:00,40\n2012-03-01T00:05:00,30\n",
                encoding="utf-8",
            )
            sink = KafkaSink()
            count, last_timestamp = producer.replay_dataset(
                sink,
                "metr-la",
                csv_path,
                limit=0,
                delay=0,
                readiness=SparkReady(frozenset({"metr-la"}), "10 minutes", "g"),
            )
        self.assertEqual(count, 2)
        self.assertEqual(last_timestamp, "2012-03-01T00:05:00")
        self.assertEqual([key for key, _ in sink.events], [b"metr-la", b"metr-la"])

    def test_out_of_order_csv_is_rejected_without_finishing_replay(self) -> None:
        class KafkaSink:
            def __init__(self) -> None:
                self.events: list[dict[str, object]] = []

            def send(self, _topic: str, *, key: bytes, value: dict[str, object]) -> None:
                self.events.append(value)

        with tempfile.TemporaryDirectory() as temporary:
            csv_path = Path(temporary) / "metr.csv"
            csv_path.write_text(
                "timestamp,s1\n2012-03-01T00:05:00,40\n2012-03-01T00:00:00,30\n",
                encoding="utf-8",
            )
            sink = KafkaSink()
            with self.assertRaisesRegex(producer.DatasetFormatError, "nondecreasing"):
                producer.replay_dataset(
                    sink,
                    "metr-la",
                    csv_path,
                    limit=0,
                    delay=0,
                    readiness=SparkReady(frozenset({"metr-la"}), "10 minutes", "g"),
                )
        self.assertEqual(len(sink.events), 1)

    def test_main_does_not_construct_producer_if_spark_is_not_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            csv_path = Path(temporary) / "metr.csv"
            csv_path.write_text("timestamp,s1\n2012-03-01T00:00:00,40\n", encoding="utf-8")
            with (
                patch.dict(producer.DATASET_FILES, {"metr-la": csv_path}),
                patch.object(producer, "wait_for_spark_ready", side_effect=TimeoutError("pending")),
                patch.object(producer, "create_producer") as create,
            ):
                result = producer.main(["--dataset", "metr-la", "--readiness-timeout", "1"])
        self.assertEqual(result, 3)
        create.assert_not_called()

    def test_main_does_not_construct_producer_if_replay_cannot_be_registered(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            csv_path = Path(temporary) / "metr.csv"
            csv_path.write_text("timestamp,s1\n2012-03-01T00:00:00,40\n", encoding="utf-8")
            with (
                patch.dict(producer.DATASET_FILES, {"metr-la": csv_path}),
                patch.object(producer, "wait_for_spark_ready", return_value=SparkReady(frozenset({"metr-la"}), "10 minutes", "g")),
                patch.object(producer, "post_json", side_effect=OSError("ledger missing")),
                patch.object(producer, "create_producer") as create,
            ):
                result = producer.main(["--dataset", "metr-la"])
        self.assertEqual(result, 3)
        create.assert_not_called()

    def test_completion_message_is_control_only_and_keeps_last_source_timestamp(self) -> None:
        marker = producer.replay_complete_event(
            "metr-la", "2012-03-01T00:00:00", "run-123"
        )
        self.assertEqual(marker["event_type"], "replay_complete")
        self.assertEqual(marker["last_event_timestamp"], "2012-03-01T00:00:00")
        self.assertNotIn("timestamp", marker)
        self.assertNotIn("readings", marker)

    def test_marker_publishing_returns_broker_acknowledged_topic_partition_offset(self) -> None:
        class Metadata:
            topic = "traffic-events"
            partition = 4
            offset = 218

        class Future:
            def get(self, *, timeout: float) -> Metadata:
                self.timeout = timeout
                return Metadata()

        class Kafka:
            def __init__(self) -> None:
                self.calls: list[tuple[str, bytes, dict[str, object]]] = []
                self.future = Future()

            def send(self, topic: str, *, key: bytes, value: dict[str, object]) -> Future:
                self.calls.append((topic, key, value))
                return self.future

        kafka = Kafka()
        metadata = producer.publish_completion_marker(kafka, "metr-la", {"event_type": "replay_complete"})
        self.assertEqual((metadata.topic, metadata.partition, metadata.offset), ("traffic-events", 4, 218))
        self.assertEqual(kafka.calls[0][:2], ("traffic-events", b"metr-la"))

    def test_completion_wait_requires_durable_complete_status(self) -> None:
        class Response:
            def __init__(self, status: str) -> None:
                self.payload = {"overall_status": status}

            def __enter__(self) -> Response:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(self.payload).encode()

        with patch("data_ingestion.producer.urlopen", return_value=Response("complete")):
            status = producer.wait_for_replay_completion(
                "http://spark/replays", "replay-a", timeout=1
            )
        self.assertEqual(status["overall_status"], "complete")

        with patch("data_ingestion.producer.urlopen", return_value=Response("pending")):
            with self.assertRaises(TimeoutError):
                producer.wait_for_replay_completion(
                    "http://spark/replays", "replay-a", timeout=0.01, poll_interval=0.002
                )


if __name__ == "__main__":
    unittest.main()
