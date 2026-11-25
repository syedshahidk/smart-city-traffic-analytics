"""Replay processed traffic snapshots to Kafka."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
import json
import logging
import math
import sys
from threading import Event
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
import uuid
from pathlib import Path
from typing import Any, Iterator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream_processing.readiness import SparkReady


PROCESSED_DIR = PROJECT_ROOT / "datasets" / "processed"
DATASET_FILES = {
    "metr-la": PROCESSED_DIR / "metr_la_cleaned.csv",
    "pems-bay": PROCESSED_DIR / "pems_bay_cleaned.csv",
}
TOPIC = "traffic-events"
LOGGER = logging.getLogger("traffic_replay")


class DatasetFormatError(ValueError):
    """Raised when a processed CSV does not match the expected schema."""


def positive_or_zero_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be a finite, non-negative number")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay processed METR-LA and/or PEMS-BAY snapshots to Kafka."
    )
    parser.add_argument(
        "--dataset",
        choices=("metr-la", "pems-bay", "both"),
        default="both",
        help="dataset to replay (default: both)",
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:29092",
        help="Kafka bootstrap server (default: localhost:29092)",
    )
    parser.add_argument(
        "--spark-readiness-url",
        default="http://127.0.0.1:8081/health/ready",
        help="Spark readiness endpoint (default: http://127.0.0.1:8081/health/ready)",
    )
    parser.add_argument(
        "--readiness-timeout",
        type=positive_or_zero_int,
        default=300,
        help="seconds to wait for all selected Spark queries to initialize (default: 300)",
    )
    parser.add_argument(
        "--replay-ledger-url",
        default="http://127.0.0.1:8081/replays",
        help="internal Spark replay ledger endpoint",
    )
    parser.add_argument(
        "--wait-for-completion", action="store_true",
        help="wait until both PostgreSQL and HDFS have verified the replay",
    )
    parser.add_argument(
        "--replay-id", type=uuid.UUID, default=None,
        help="stable UUID for an intentional retry/resume; omitted IDs are generated",
    )
    parser.add_argument(
        "--completion-timeout", type=positive_or_zero_int, default=1800,
        help="seconds to wait when --wait-for-completion is used (default: 1800)",
    )
    parser.add_argument(
        "--threshold", type=float, default=20.0,
        help="congestion speed threshold; must match the Spark query",
    )
    parser.add_argument(
        "--delay",
        type=non_negative_float,
        default=1.0,
        help="seconds between events (default: 1)",
    )
    parser.add_argument(
        "--limit",
        type=positive_or_zero_int,
        default=10,
        help="maximum events per selected dataset; 0 replays all rows (default: 10)",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    return parser.parse_args(argv)


def selected_datasets(selection: str) -> list[str]:
    return list(DATASET_FILES) if selection == "both" else [selection]


def wait_for_spark_ready(
    readiness_url: str,
    datasets: set[str] | frozenset[str],
    *,
    timeout: float,
    poll_interval: float = 0.25,
) -> SparkReady:
    """Wait for current Spark query progress before any Kafka producer is made."""
    if timeout <= 0 or poll_interval <= 0:
        raise ValueError("readiness timeout and poll interval must be positive")
    requested = frozenset(datasets)
    if not requested or not requested <= set(DATASET_FILES):
        raise ValueError("readiness requires supported dataset names")
    deadline = time.monotonic() + timeout
    last_problem = "Spark readiness endpoint is not available"
    wait_event = Event()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"timed out waiting for Spark readiness for {', '.join(sorted(requested))}: "
                f"{last_problem}"
            )
        query = urlencode({"datasets": ",".join(sorted(requested))})
        separator = "&" if "?" in readiness_url else "?"
        try:
            with urlopen(f"{readiness_url}{separator}{query}", timeout=min(5.0, remaining)) as response:
                payload = json.loads(response.read().decode("utf-8"))
                ready = frozenset(payload.get("ready_datasets", []))
                if response.status == 200 and payload.get("status") == "ready" and requested <= ready:
                    return SparkReady(
                        datasets=ready,
                        watermark_delay=str(payload["watermark_delay"]),
                        generation=str(payload["generation"]),
                    )
                detail = payload.get("diagnostics")
                last_problem = (
                    json.dumps(detail, sort_keys=True)
                    if detail is not None
                    else str(payload.get("message") or payload.get("status") or "not ready")
                )
        except HTTPError as exc:
            try:
                payload = json.loads(exc.read().decode("utf-8"))
                detail = payload.get("diagnostics")
                last_problem = (
                    json.dumps(detail, sort_keys=True)
                    if detail is not None
                    else str(payload.get("status", exc))
                )
            except (ValueError, AttributeError):
                last_problem = str(exc)
        except (URLError, TimeoutError, OSError, ValueError, KeyError) as exc:
            last_problem = str(exc)
        wait_event.wait(min(poll_interval, max(0.0, deadline - time.monotonic())))


def _speed(value: str, *, dataset: str, row_number: int, sensor_id: str) -> float | None:
    # A blank is the CSV representation of a missing value; keep it explicit as
    # JSON null. Numeric zero remains 0.0 and is never treated as missing.
    if value == "":
        return None
    try:
        result = float(value)
    except ValueError as exc:
        raise DatasetFormatError(
            f"{dataset}: invalid speed at CSV row {row_number}, sensor {sensor_id!r}: {value!r}"
        ) from exc
    if not math.isfinite(result):
        raise DatasetFormatError(
            f"{dataset}: non-finite speed at CSV row {row_number}, sensor {sensor_id!r}"
        )
    return result


def iter_events(dataset: str, csv_path: Path, limit: int = 10) -> Iterator[dict[str, Any]]:
    """Read one CSV row at a time and yield its traffic snapshot."""
    if dataset not in DATASET_FILES:
        raise ValueError(f"unsupported dataset {dataset!r}")
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if not csv_path.is_file():
        raise FileNotFoundError(f"processed dataset not found: {csv_path}")

    try:
        source = csv_path.open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise OSError(f"cannot open processed dataset {csv_path}: {exc}") from exc

    with source:
        reader = csv.reader(source)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise DatasetFormatError(f"{csv_path} is empty") from exc

        if len(header) < 2 or header[0].strip().lower() != "timestamp":
            raise DatasetFormatError(
                f"{csv_path} must start with a 'timestamp' column and at least one sensor"
            )
        if any(not sensor_id for sensor_id in header[1:]):
            raise DatasetFormatError(f"{csv_path} contains an empty sensor ID")

        sensor_ids = header[1:]
        emitted = 0
        for row_number, row in enumerate(reader, start=2):
            if limit and emitted >= limit:
                break
            if len(row) != len(header):
                raise DatasetFormatError(
                    f"{csv_path}: row {row_number} has {len(row)} fields; expected {len(header)}"
                )
            timestamp = row[0].strip()
            if not timestamp:
                raise DatasetFormatError(f"{csv_path}: missing timestamp at row {row_number}")
            try:
                timestamp = datetime.fromisoformat(timestamp).isoformat()
            except ValueError as exc:
                raise DatasetFormatError(
                    f"{csv_path}: invalid ISO timestamp at row {row_number}: {row[0]!r}"
                ) from exc
            readings = [
                {
                    "sensor_id": sensor_id,
                    "speed": _speed(
                        raw_speed,
                        dataset=dataset,
                        row_number=row_number,
                        sensor_id=sensor_id,
                    ),
                }
                for sensor_id, raw_speed in zip(sensor_ids, row[1:])
            ]
            yield {
                "event_type": "traffic_snapshot",
                "dataset": dataset,
                "timestamp": timestamp,
                "sensor_count": len(sensor_ids),
                "readings": readings,
            }
            emitted += 1


def json_serializer(event: dict[str, Any]) -> bytes:
    return json.dumps(event, separators=(",", ":"), allow_nan=False).encode("utf-8")


def create_producer(bootstrap_server: str) -> Any:
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
    except ImportError as exc:
        raise RuntimeError(
            "Kafka producer dependency is unavailable. Install requirements.txt "
            "(kafka-python) in the active Python environment."
        ) from exc

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            value_serializer=json_serializer,
            compression_type="gzip",
            acks="all",
            max_block_ms=10_000,
            request_timeout_ms=15_000,
        )
        return producer, KafkaError
    except (KafkaError, OSError, TimeoutError) as exc:
        raise ConnectionError(
            f"could not initialize Kafka producer for {bootstrap_server}: {exc}"
        ) from exc


def replay_dataset(
    producer: Any,
    dataset: str,
    csv_path: Path,
    *,
    limit: int,
    delay: float,
    readiness: SparkReady,
) -> tuple[int, str | None]:
    if dataset not in readiness.datasets:
        raise RuntimeError(f"Spark has not confirmed readiness for {dataset}")
    published = 0
    last_timestamp: str | None = None
    previous_event_time: datetime | None = None
    for event in iter_events(dataset, csv_path, limit):
        parsed_event_time = datetime.fromisoformat(event["timestamp"])
        if parsed_event_time.tzinfo is None:
            parsed_event_time = parsed_event_time.replace(tzinfo=timezone.utc)
        else:
            parsed_event_time = parsed_event_time.astimezone(timezone.utc)
        if previous_event_time is not None and parsed_event_time < previous_event_time:
            raise DatasetFormatError(
                f"{dataset}: timestamps must be nondecreasing to safely finalize a replay"
            )
        if published and delay:
            time.sleep(delay)
        try:
            # Keep snapshots for one dataset and its completion marker on the
            # same Kafka partition, preserving their order across partitions.
            producer.send(TOPIC, key=dataset.encode("utf-8"), value=event)
        except Exception as exc:
            raise ConnectionError(
                f"failed to publish {dataset} event at {event['timestamp']}: {exc}"
            ) from exc
        published += 1
        last_timestamp = event["timestamp"]
        previous_event_time = parsed_event_time
        if published == 1 or published % 100 == 0:
            LOGGER.info("Published %d %s snapshots", published, dataset)
    return published, last_timestamp


def replay_complete_event(
    dataset: str, last_event_timestamp: str, replay_id: str,
    manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a control record; its timestamp is never treated as traffic data."""
    event: dict[str, Any] = {
        "event_type": "replay_complete",
        "dataset": dataset,
        "replay_id": replay_id,
        "last_event_timestamp": last_event_timestamp,
    }
    if manifest is not None:
        event.update({
            "expected_last_source_timestamp": manifest["expected_last_source_timestamp"].isoformat(),
            "expected_final_window_start": manifest["expected_final_window_start"].isoformat(),
            "expected_final_window_end": manifest["expected_final_window_end"].isoformat(),
        })
    return event


def publish_completion_marker(producer: Any, dataset: str, control: dict[str, Any], *, timeout: float = 30) -> Any:
    """Wait for broker acknowledgment and return its authoritative coordinates."""
    future = producer.send(TOPIC, key=dataset.encode("utf-8"), value=control)
    if future is None or not callable(getattr(future, "get", None)):
        raise RuntimeError("Kafka producer did not return a delivery future for the marker")
    return future.get(timeout=timeout)


def _utc_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_replay_manifest(
    dataset: str, csv_path: Path, *, limit: int, replay_id: str, threshold: float
) -> dict[str, Any]:
    """Pre-scan the selected source rows and derive exact expected final-window metrics."""
    if not math.isfinite(threshold):
        raise ValueError("threshold must be finite")
    last_time: datetime | None = None
    previous_time: datetime | None = None
    previous_event: dict[str, Any] | None = None
    final_start: datetime | None = None
    final_values: list[float] = []
    for event in iter_events(dataset, csv_path, limit):
        event_time = _utc_timestamp(event["timestamp"])
        if previous_time is not None and event_time < previous_time:
            raise DatasetFormatError(f"{dataset}: timestamps must be nondecreasing")
        if previous_time == event_time:
            if previous_event != event:
                raise DatasetFormatError(
                    f"{dataset}: conflicting snapshots share timestamp {event['timestamp']}"
                )
            continue
        window_epoch = int(event_time.timestamp()) // 300 * 300
        window_start = datetime.fromtimestamp(window_epoch, tz=timezone.utc)
        if final_start != window_start:
            final_start = window_start
            final_values = []
        final_values.extend(
            float(reading["speed"])
            for reading in event["readings"]
            if reading["speed"] is not None
        )
        last_time = event_time
        previous_time = event_time
        previous_event = event
    if last_time is None or final_start is None:
        raise DatasetFormatError(f"{dataset}: replay contains no traffic snapshots")
    if not final_values:
        raise DatasetFormatError(
            f"{dataset}: final five-minute window has no non-null readings to verify"
        )
    congested = sum(value < threshold for value in final_values)
    return {
        "replay_id": replay_id,
        "dataset": dataset,
        "expected_last_source_timestamp": last_time,
        "expected_final_window_start": final_start,
        "expected_final_window_end": final_start + timedelta(minutes=5),
        "expected_average_speed": sum(final_values) / len(final_values),
        "expected_total_sensor_readings": len(final_values),
        "expected_congested_sensor_count": congested,
        "expected_congestion_percentage": congested * 100.0 / len(final_values),
    }


def post_json(url: str, payload: dict[str, Any], *, timeout: float = 10.0) -> dict[str, Any]:
    from urllib.request import Request

    request = Request(
        url,
        data=json.dumps(payload, separators=(",", ":"), default=lambda value: value.isoformat()).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_replay_completion(
    ledger_url: str, replay_id: str, *, timeout: float, poll_interval: float = 1.0
) -> dict[str, Any]:
    """Poll durable state; a timeout is never treated as successful completion."""
    if timeout <= 0 or poll_interval <= 0:
        raise ValueError("completion timeout and poll interval must be positive")
    status_url = ledger_url.rstrip("/") + "/" + replay_id
    deadline = time.monotonic() + timeout
    last_payload: dict[str, Any] = {}
    while time.monotonic() < deadline:
        with urlopen(status_url, timeout=min(5.0, max(0.1, deadline - time.monotonic()))) as response:
            last_payload = json.loads(response.read().decode("utf-8"))
        if last_payload.get("overall_status") == "complete":
            return last_payload
        if last_payload.get("overall_status") == "failed":
            raise RuntimeError(last_payload.get("status_reason") or "replay verification failed")
        time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
    raise TimeoutError(
        f"replay {replay_id} did not reach verified completion before timeout; "
        f"last status: {last_payload.get('overall_status', 'unknown')}"
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    datasets = selected_datasets(args.dataset)
    missing = [str(DATASET_FILES[name]) for name in datasets if not DATASET_FILES[name].is_file()]
    if missing:
        LOGGER.error("Processed dataset file(s) not found: %s", ", ".join(missing))
        return 2

    if not math.isfinite(args.threshold):
        LOGGER.error("Congestion threshold must be finite")
        return 2
    manifests: dict[str, dict[str, Any]] = {}
    try:
        for dataset in datasets:
            replay_id = (
                str(args.replay_id) if args.replay_id is not None and len(datasets) == 1
                else str(uuid.uuid5(args.replay_id, dataset)) if args.replay_id is not None
                else uuid.uuid4().hex
            )
            manifests[dataset] = build_replay_manifest(
                dataset,
                DATASET_FILES[dataset],
                limit=args.limit,
                replay_id=replay_id,
                threshold=args.threshold,
            )
    except (DatasetFormatError, OSError, ValueError) as exc:
        LOGGER.error("Replay manifest could not be prepared: %s", exc)
        return 2

    try:
        readiness = wait_for_spark_ready(
            args.spark_readiness_url,
            set(datasets),
            timeout=args.readiness_timeout,
        )
    except (TimeoutError, ValueError) as exc:
        LOGGER.error("Replay not started because Spark is not ready: %s", exc)
        return 3
    LOGGER.info(
        "Spark is ready for %s (generation=%s, watermark=%s)",
        ", ".join(sorted(readiness.datasets)),
        readiness.generation,
        readiness.watermark_delay,
    )

    registered_runs: dict[str, dict[str, Any]] = {}
    try:
        for manifest in manifests.values():
            registered = post_json(args.replay_ledger_url, manifest)
            if str(registered.get("replay_id")) != str(manifest["replay_id"]):
                raise RuntimeError("replay ledger returned a different replay_id")
            registered_runs[str(manifest["dataset"])] = registered
            LOGGER.info(
                "Registered replay_id=%s dataset=%s final_window=[%s,%s)",
                manifest["replay_id"], manifest["dataset"],
                manifest["expected_final_window_start"], manifest["expected_final_window_end"],
            )
    except (OSError, TimeoutError, ValueError, KeyError, RuntimeError) as exc:
        LOGGER.error("Replay not started because the durable ledger is unavailable: %s", exc)
        return 3

    pending_publish = [
        dataset for dataset in datasets
        if registered_runs[dataset].get("marker_status") != "observed"
        and registered_runs[dataset].get("overall_status") != "complete"
    ]
    if not pending_publish:
        if args.wait_for_completion:
            try:
                for manifest in manifests.values():
                    wait_for_replay_completion(
                        args.replay_ledger_url,
                        str(manifest["replay_id"]),
                        timeout=args.completion_timeout,
                    )
            except (OSError, TimeoutError, RuntimeError, ValueError) as exc:
                LOGGER.error("Replay is not yet fully verified: %s", exc)
                return 3
        else:
            LOGGER.info("All selected replay markers were already observed; no events republished")
        return 0

    try:
        producer, kafka_error = create_producer(args.bootstrap_server)
    except RuntimeError as exc:
        LOGGER.error("%s", exc)
        return 2
    except ConnectionError as exc:
        LOGGER.error("%s", exc)
        return 3

    result = 0
    try:
        for dataset in datasets:
            existing = registered_runs[dataset]
            if existing.get("marker_status") == "observed" or existing.get("overall_status") == "complete":
                LOGGER.info("Skipping already marked replay_id=%s dataset=%s", manifests[dataset]["replay_id"], dataset)
                if args.wait_for_completion and existing.get("overall_status") != "complete":
                    wait_for_replay_completion(
                        args.replay_ledger_url,
                        str(manifests[dataset]["replay_id"]),
                        timeout=args.completion_timeout,
                    )
                continue
            path = DATASET_FILES[dataset]
            LOGGER.info(
                "Starting %s replay from %s (limit=%s, delay=%ss)",
                dataset,
                path,
                "all" if args.limit == 0 else args.limit,
                args.delay,
            )
            count, last_timestamp = replay_dataset(
                producer,
                dataset,
                path,
                limit=args.limit,
                delay=args.delay,
                readiness=readiness,
            )
            try:
                producer.flush(timeout=30)
            except kafka_error as exc:
                raise ConnectionError(f"Kafka flush failed after {dataset}: {exc}") from exc
            LOGGER.info("Finished %s replay: %d events", dataset, count)
            if count and last_timestamp is not None:
                manifest = manifests[dataset]
                control = replay_complete_event(
                    dataset, last_timestamp, str(manifest["replay_id"]), manifest
                )
                try:
                    metadata = publish_completion_marker(producer, dataset, control)
                except Exception as exc:
                    raise ConnectionError(
                        f"failed to publish {dataset} replay completion signal: {exc}"
                    ) from exc
                try:
                    producer.flush(timeout=30)
                except kafka_error as exc:
                    raise ConnectionError(
                        f"Kafka flush failed after {dataset} replay completion signal: {exc}"
                    ) from exc
                try:
                    post_json(
                        args.replay_ledger_url.rstrip("/") + "/" + str(manifest["replay_id"]) + "/marker",
                        {
                            "topic": metadata.topic,
                            "partition": metadata.partition,
                            "offset": metadata.offset,
                        },
                    )
                except (OSError, TimeoutError, ValueError, KeyError) as exc:
                    # Spark's durable marker observer can recover this acknowledgment
                    # after the producer exits; report it so the operator checks status.
                    raise ConnectionError(
                        f"Kafka acknowledged marker for {dataset}, but ledger acknowledgment "
                        f"failed; replay_id={manifest['replay_id']}: {exc}"
                    ) from exc
                LOGGER.info(
                    "Published replay completion marker for %s at %s/%s/%s; replay_id=%s",
                    dataset, metadata.topic, metadata.partition, metadata.offset, manifest["replay_id"],
                )
                if args.wait_for_completion:
                    wait_for_replay_completion(
                        args.replay_ledger_url,
                        str(manifest["replay_id"]),
                        timeout=args.completion_timeout,
                    )
    except (DatasetFormatError, OSError, ConnectionError, RuntimeError, TimeoutError, ValueError) as exc:
        LOGGER.error("Replay stopped: %s", exc)
        result = 3 if isinstance(exc, ConnectionError) else 2
    except KeyboardInterrupt:
        LOGGER.info("Replay interrupted; flushing queued events before shutdown")
        try:
            producer.flush(timeout=30)
        except kafka_error as exc:
            LOGGER.error("Kafka flush failed during shutdown: %s", exc)
            result = 3
    finally:
        try:
            producer.flush(timeout=30)
        except kafka_error as exc:
            LOGGER.error("Kafka flush failed during shutdown: %s", exc)
            result = 3
        finally:
            try:
                producer.close(timeout=30)
            except kafka_error as exc:
                LOGGER.error("Kafka producer shutdown failed: %s", exc)
                result = 3
    return result


if __name__ == "__main__":
    sys.exit(main())
