"""Compute windowed traffic metrics from the Kafka traffic-events topic.

The matching Kafka connector is resolved by Spark when this script creates its
SparkSession, so it can be launched directly with the project Python, for example:

    .venv\\Scripts\\python.exe stream_processing\\traffic_stream.py --dataset metr-la

Five-minute aggregates are persisted as finalized windows through PostgreSQL
Append mode so the watermark-bounded snapshot deduplicator can precede the
window aggregation. The foreachBatch callback still performs idempotent upserts.
HDFS also receives finalized windows in Append mode; each sink has an independent,
versioned checkpoint for the deduplicated query.
"""

from __future__ import annotations

import argparse
from itertools import islice
import logging
import math
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any

from stream_processing.readiness import ReadinessState, start_readiness_server
from stream_processing.replay_ledger import (
    PostgresReplayAPI,
    HdfsReplayVerifier,
    observe_marker,
    pending_postgres_replays,
    reconcile_postgres_candidates,
    verify_postgres_final_window,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CHECKPOINT_BASE = PROJECT_ROOT / "storage" / "checkpoints" / "traffic_stream_deduplicated_v2"
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
IVY_CACHE_DIR = "/tmp/.ivy2.5.2"
HDFS_URI = "hdfs://namenode:8020"
LOGGER = logging.getLogger("traffic_stream")


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def finite_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a finite number") from exc
    if not math.isfinite(parsed):
        raise argparse.ArgumentTypeError("must be a finite number")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate five-minute traffic metrics from Kafka snapshots."
    )
    parser.add_argument(
        "--dataset",
        choices=("metr-la", "pems-bay", "both"),
        default="both",
        help="dataset to process; both starts two independently watermarked queries",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:29092",
        help="Kafka bootstrap servers (default: localhost:29092)",
    )
    parser.add_argument(
        "--topic",
        default="traffic-events",
        help="Kafka source topic (default: traffic-events)",
    )
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default="latest",
        help="offset for a new checkpoint (default: latest; use earliest only for an intentional replay)",
    )
    parser.add_argument(
        "--threshold",
        type=finite_float,
        default=20.0,
        help="a reading below this speed is counted as congested (default: 20.0)",
    )
    parser.add_argument(
        "--watermark-delay",
        default="10 minutes",
        help="event-time lateness allowed by the watermark (default: 10 minutes)",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=DEFAULT_CHECKPOINT_BASE,
        help="base directory for per-dataset persistent checkpoints",
    )
    parser.add_argument(
        "--master",
        default="local[2]",
        help="Spark master URL (default: local[2])",
    )
    parser.add_argument(
        "--driver-memory",
        default="1g",
        help="Spark driver memory (default: 1g)",
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=positive_int,
        default=100,
        help="maximum Kafka records read per micro-batch (default: 100)",
    )
    parser.add_argument(
        "--readiness-host",
        default="0.0.0.0",
        help="HTTP readiness bind address (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--readiness-port",
        type=positive_int,
        default=8081,
        help="HTTP readiness port (default: 8081)",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    return parser.parse_args(argv)


def selected_datasets(selection: str) -> list[str]:
    """Expand both into independent per-dataset queries."""
    if selection == "both":
        return ["metr-la", "pems-bay"]
    if selection in {"metr-la", "pems-bay"}:
        return [selection]
    raise ValueError(f"unsupported dataset selection {selection!r}")


def checkpoint_path(base_dir: Path, dataset: str, sink: str = "postgres") -> Path:
    """Return a v2 checkpoint isolated by dataset and sink."""
    if dataset not in {"metr-la", "pems-bay"} or sink not in {"postgres", "hdfs", "replay_ledger"}:
        raise ValueError("checkpoint requires a supported dataset and sink")
    return base_dir.expanduser().resolve() / dataset / sink


def hdfs_metrics_path(dataset: str) -> str:
    """Return a new append-only output path for the v2 stateful query plan."""
    if dataset not in {"metr-la", "pems-bay"}:
        raise ValueError(f"unsupported dataset {dataset!r}")
    return f"{HDFS_URI}/traffic/streaming/metrics-deduplicated-v2/{dataset}"


def hdfs_checkpoint_path(dataset: str) -> str:
    """Return the HDFS checkpoint, independent from PostgreSQL's checkpoint."""
    if dataset not in {"metr-la", "pems-bay"}:
        raise ValueError(f"unsupported dataset {dataset!r}")
    return f"{HDFS_URI}/traffic/system/checkpoints/streaming-metrics-deduplicated-v2/{dataset}/hdfs"


def watermark_advance_microseconds(watermark_delay: str) -> int:
    """Return replay-control advance: window length + delay + one second."""
    match = re.fullmatch(
        r"\s*(\d+)\s+(milliseconds?|seconds?|minutes?|hours?|days?)\s*",
        watermark_delay,
        re.I,
    )
    if not match:
        raise ValueError("watermark delay must be an integer duration such as '10 minutes'")
    amount = int(match.group(1))
    unit = match.group(2).lower()
    unit_microseconds = {
        "millisecond": 1_000,
        "milliseconds": 1_000,
        "second": 1_000_000,
        "seconds": 1_000_000,
        "minute": 60_000_000,
        "minutes": 60_000_000,
        "hour": 3_600_000_000,
        "hours": 3_600_000_000,
        "day": 86_400_000_000,
        "days": 86_400_000_000,
    }
    return 300_000_000 + amount * unit_microseconds[unit] + 1_000_000


def query_failure_messages(queries: list[Any]) -> list[str]:
    """Collect available failure details from terminated streaming queries."""
    failures = []
    for query in queries:
        try:
            error = query.exception()
        except Exception as exc:
            failures.append(f"{query.name}: could not read query exception: {exc}")
            continue
        if error is not None:
            failures.append(f"{query.name}: {error}")
    return failures


def build_event_schema() -> Any:
    """Build the producer JSON schema, importing PySpark only when needed."""
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    reading_schema = StructType(
        [
            StructField("sensor_id", StringType(), nullable=False),
            StructField("speed", DoubleType(), nullable=True),
        ]
    )
    return StructType(
        [
            StructField("event_type", StringType(), nullable=True),
            StructField("dataset", StringType(), nullable=True),
            StructField("timestamp", StringType(), nullable=True),
            StructField("last_event_timestamp", StringType(), nullable=True),
            StructField("replay_id", StringType(), nullable=True),
            StructField("expected_last_source_timestamp", StringType(), nullable=True),
            StructField("expected_final_window_start", StringType(), nullable=True),
            StructField("expected_final_window_end", StringType(), nullable=True),
            StructField("sensor_count", IntegerType(), nullable=True),
            StructField("readings", ArrayType(reading_schema), nullable=True),
        ]
    )


def build_replay_marker_stream(events: Any, dataset: str) -> Any:
    """Select completion records and retain Kafka coordinates for durable recovery."""
    if dataset not in {"metr-la", "pems-bay"}:
        raise ValueError("replay marker stream requires one dataset")
    from pyspark.sql import functions as F

    parsed = events.select(
        F.col("topic"), F.col("partition"), F.col("offset"),
        F.from_json(F.col("value").cast("string"), build_event_schema()).alias("event"),
    ).where(F.col("event").isNotNull())
    selected = parsed.where(F.col("event.dataset") == dataset)
    return selected.where(
        (F.col("event.event_type") == "replay_complete")
        & F.col("event.replay_id").isNotNull()
        & F.col("event.last_event_timestamp").isNotNull()
    ).select(
        F.col("event.replay_id").alias("replay_id"),
        F.col("event.dataset").alias("dataset"),
        F.col("event.last_event_timestamp").alias("last_event_timestamp"),
        F.col("topic"), F.col("partition"), F.col("offset"),
    )


def persist_replay_markers(
    batch_df: Any, batch_id: int, postgres_checkpoint_dir: Path | None = None
) -> None:
    """Record marker coordinates transactionally; retries are idempotent."""
    rows = list(batch_df.toLocalIterator())
    if not rows:
        LOGGER.debug("Skipping empty replay-marker batch %s", batch_id)
        return
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError("psycopg2-binary is required for the replay ledger") from exc
    connection = open_postgres_connection(psycopg2)
    try:
        with connection:
            datasets: set[str] = set()
            for row in rows:
                if not observe_marker(connection, row.asDict(recursive=True)):
                    LOGGER.warning(
                        "Ignoring unregistered or conflicting replay marker %s at %s/%s/%s",
                        row.replay_id, row.topic, row.partition, row.offset,
                    )
                    continue
                datasets.add(str(row.dataset))
            if postgres_checkpoint_dir is not None:
                for selected_dataset in datasets:
                    reconcile_postgres_candidates(
                        connection, selected_dataset, postgres_checkpoint_dir
                    )
    finally:
        connection.close()
    LOGGER.info("Observed %d replay completion markers in batch %s", len(rows), batch_id)


def build_aggregates(
    events: Any,
    threshold: float,
    dataset: str = "both",
    watermark_delay: str = "10 minutes",
) -> Any:
    """Build one dataset's stateful plan; `both` is forbidden to isolate watermarks."""
    if dataset not in {"metr-la", "pems-bay"}:
        raise ValueError("build_aggregates requires one dataset to keep watermark state isolated")
    from pyspark.sql import functions as F

    marker_advance_us = watermark_advance_microseconds(watermark_delay)

    parsed = events.select(
        F.from_json(F.col("value").cast("string"), build_event_schema()).alias("event")
    ).where(F.col("event").isNotNull())

    selected = parsed.select("event.*").where(F.col("dataset") == dataset)
    snapshots = selected.where(
        (F.col("event_type") == F.lit("traffic_snapshot"))
        & F.col("timestamp").isNotNull()
    )
    replay_completions = selected.where(
        (F.col("event_type") == F.lit("replay_complete"))
        & F.col("last_event_timestamp").isNotNull()
        & F.col("replay_id").isNotNull()
    )
    if threshold != threshold or threshold in (float("inf"), float("-inf")):
        raise ValueError("congestion threshold must be a finite number")

    event_schema = build_event_schema()
    empty_readings = F.array().cast(event_schema["readings"].dataType)
    snapshot_events = snapshots.select(
        F.col("dataset"),
        F.to_timestamp(F.col("timestamp")).alias("event_time"),
        F.col("readings"),
        F.lit(False).alias("is_replay_control"),
    ).where(F.col("event_time").isNotNull())
    control_events = replay_completions.select(
        F.col("dataset"),
        (
            F.to_timestamp(F.col("last_event_timestamp"))
            + F.expr(f"INTERVAL {marker_advance_us} MICROSECONDS")
        ).alias("event_time"),
        empty_readings.alias("readings"),
        F.lit(True).alias("is_replay_control"),
    ).where(F.col("event_time").isNotNull())
    unique_events = (
        snapshot_events.unionByName(control_events)
        # Retain one complete event per dataset/timestamp through the event-time
        # lateness horizon, before one snapshot becomes many sensor rows. The
        # completion control shares this watermark solely to release trailing state.
        .withWatermark("event_time", watermark_delay)
        .dropDuplicatesWithinWatermark(["dataset", "event_time"])
    )

    sensor_rows = (
        unique_events.select(
            "dataset",
            "event_time",
            "is_replay_control",
            F.explode_outer(F.col("readings")).alias("reading"),
        )
        .select(
            "dataset",
            "event_time",
            F.col("reading.sensor_id").alias("sensor_id"),
            F.col("reading.speed").cast("double").alias("speed"),
            "is_replay_control",
        )
    )

    return (
        # Apply the second watermark before filtering control rows. Spark sees
        # their event time to advance the window watermark, but the rows cannot
        # contribute to any metric.
        sensor_rows.withWatermark("event_time", watermark_delay)
        .where((~F.col("is_replay_control")) & F.col("speed").isNotNull())
        .groupBy(
            F.window(F.col("event_time"), "5 minutes").alias("window"),
            F.col("dataset"),
        )
        .agg(
            F.avg("speed").alias("average_speed"),
            F.count("speed").alias("total_sensor_readings"),
            F.sum(F.when(F.col("speed") < threshold, 1).otherwise(0)).alias(
                "congested_sensor_count"
            ),
        )
        .withColumn(
            "congestion_percentage",
            F.when(
                F.col("total_sensor_readings") > 0,
                F.col("congested_sensor_count")
                * F.lit(100.0)
                / F.col("total_sensor_readings"),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            F.col("dataset"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("average_speed"),
            F.col("total_sensor_readings"),
            F.col("congested_sensor_count"),
            F.col("congestion_percentage"),
        )
    )


def persist_batch(
    batch_df: Any, batch_id: int, postgres_checkpoint_dir: Path | None = None
) -> None:
    """Persist changed metrics from one Spark micro-batch using idempotent upserts."""
    columns = [
        "dataset",
        "window_start",
        "window_end",
        "average_speed",
        "total_sensor_readings",
        "congested_sensor_count",
        "congestion_percentage",
    ]
    batch_rows = list(batch_df.select(*columns).toLocalIterator())
    if not batch_rows:
        LOGGER.debug("Skipping empty Spark micro-batch %s", batch_id)
        return

    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2-binary is required in the Spark container to persist metrics"
        ) from exc

    connection = open_postgres_connection(psycopg2)
    upsert_sql = """
        INSERT INTO traffic_metrics (
            dataset, window_start, window_end, average_speed,
            total_sensor_readings, congested_sensor_count,
            congestion_percentage
        ) VALUES %s
        ON CONFLICT (dataset, window_start, window_end) DO UPDATE SET
            average_speed = EXCLUDED.average_speed,
            total_sensor_readings = EXCLUDED.total_sensor_readings,
            congested_sensor_count = EXCLUDED.congested_sensor_count,
            congestion_percentage = EXCLUDED.congestion_percentage,
            processed_at = CURRENT_TIMESTAMP
    """
    persisted = 0
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'UTC'")
                pending = iter(batch_rows)
                while row_chunk := list(islice(pending, 500)):
                    values = [tuple(row) for row in row_chunk]
                    execute_values(cursor, upsert_sql, values, page_size=500)
                    persisted += len(values)
                if postgres_checkpoint_dir is not None:
                    datasets = {str(row["dataset"]) for row in batch_rows}
                    for dataset in datasets:
                        for replay in pending_postgres_replays(connection, dataset):
                            verify_postgres_final_window(
                                connection,
                                replay,
                                batch_id,
                                batch_rows,
                                postgres_checkpoint_dir / "offsets" / str(batch_id),
                            )
    finally:
        connection.close()

    LOGGER.info(
        "Upserted %s traffic metric rows from Spark micro-batch %s",
        persisted,
        batch_id,
    )


CREATE_METRICS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS traffic_metrics (
        id BIGSERIAL PRIMARY KEY,
        dataset TEXT NOT NULL,
        window_start TIMESTAMPTZ NOT NULL,
        window_end TIMESTAMPTZ NOT NULL,
        average_speed DOUBLE PRECISION NOT NULL,
        total_sensor_readings BIGINT NOT NULL,
        congested_sensor_count BIGINT NOT NULL,
        congestion_percentage DOUBLE PRECISION NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT uq_traffic_metrics_window
            UNIQUE (dataset, window_start, window_end)
    )
"""


def open_postgres_connection(psycopg2: Any) -> Any:
    required = (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    )
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError(
            "Missing PostgreSQL environment variable(s): " + ", ".join(missing)
        )

    try:
        return psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=10,
            options="-c timezone=UTC",
        )
    except (ValueError, psycopg2.Error) as exc:
        raise RuntimeError(
            "Could not connect to PostgreSQL at "
            f"{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')} "
            f"database {os.environ.get('POSTGRES_DB')!r}: {exc}"
        ) from exc


def initialize_metrics_table() -> None:
    """Create the metrics table and its retry-safe uniqueness constraint."""
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2-binary is required in the Spark container to persist metrics"
        ) from exc

    connection = open_postgres_connection(psycopg2)
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_METRICS_TABLE_SQL)
    finally:
        connection.close()
    LOGGER.info("PostgreSQL table traffic_metrics is ready")


def create_spark_session(SparkSession: Any, args: argparse.Namespace) -> Any:
    """Create Spark and resolve the Kafka connector for direct Python launches."""
    LOGGER.info("Resolving Spark Kafka connector: %s", KAFKA_CONNECTOR)
    builder = (
        SparkSession.builder.appName("SmartCityTrafficMetrics")
        .master(args.master)
        # Spark resolves this Maven coordinate while creating the SparkContext.
        # This works for `python traffic_stream.py`, without spark-submit --packages.
        .config("spark.jars.packages", KAFKA_CONNECTOR)
        .config("spark.jars.ivy", IVY_CACHE_DIR)
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
    )
    try:
        return builder.getOrCreate()
    except Exception as exc:
        detail = str(exc)
        lowered = detail.lower()
        dependency_failure = any(
            marker in lowered
            for marker in (
                "spark-sql-kafka",
                "unresolved dependency",
                "could not resolve dependencies",
                "resolveexception",
                "artifact not found",
                "failed to resolve",
            )
        )
        if dependency_failure:
            raise RuntimeError(
                f"Could not download the required Spark Kafka connector "
                f"{KAFKA_CONNECTOR}. Check Maven Central/network access and the "
                f"Spark Ivy cache. Original error: {detail}"
            ) from exc
        raise


def run(args: argparse.Namespace) -> int:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        LOGGER.error("PySpark is unavailable; install pyspark==4.0.0 in the Spark environment")
        raise RuntimeError("PySpark is required to run this streaming job") from exc

    if args.threshold != args.threshold or args.threshold in (float("inf"), float("-inf")):
        raise ValueError("congestion threshold must be a finite number")
    if watermark_advance_microseconds(args.watermark_delay) <= 0:
        raise ValueError("watermark delay must be positive")

    initialize_metrics_table()
    datasets = selected_datasets(args.dataset)
    generation = uuid.uuid4().hex
    LOGGER.info(
        "Starting independent Spark Structured Streaming queries: datasets=%s topic=%s "
        "broker=%s offsets=%s threshold=%s checkpoint_base=%s",
        args.dataset,
        args.topic,
        args.bootstrap_servers,
        args.starting_offsets,
        args.threshold,
        args.checkpoint_dir,
    )

    spark = create_spark_session(SparkSession, args)
    spark.sparkContext.setLogLevel("WARN")

    queries: list[Any] = []
    readiness = ReadinessState(set(datasets), args.watermark_delay, generation)
    readiness_server = None
    try:
        def ledger_connection() -> Any:
            import psycopg2
            return open_postgres_connection(psycopg2)

        hdfs_verifier = HdfsReplayVerifier(
            spark,
            ledger_connection,
            {dataset: hdfs_checkpoint_path(dataset) for dataset in datasets},
            {dataset: hdfs_metrics_path(dataset) for dataset in datasets},
        )

        def reconcile_replay(replay: dict[str, Any]) -> None:
            pg_checkpoint_for_dataset = checkpoint_path(
                args.checkpoint_dir, replay["dataset"], "postgres"
            )
            if replay["postgres_status"] == "pending":
                connection = ledger_connection()
                try:
                    with connection:
                        reconcile_postgres_candidates(
                            connection, replay["dataset"], pg_checkpoint_for_dataset
                        )
                finally:
                    connection.close()
            if replay["hdfs_status"] == "pending":
                hdfs_verifier.verify(replay)

        readiness_server, _server_thread = start_readiness_server(
            readiness, args.readiness_host, args.readiness_port,
            replay_api=PostgresReplayAPI(ledger_connection, reconcile_replay),
        )
        for dataset in datasets:
            pg_checkpoint = checkpoint_path(args.checkpoint_dir, dataset, "postgres")
            hdfs_checkpoint = hdfs_checkpoint_path(dataset)
            hdfs_output = hdfs_metrics_path(dataset)
            LOGGER.info(
                "Starting isolated dataset query: dataset=%s postgres_checkpoint=%s "
                "hdfs_checkpoint=%s hdfs_output=%s",
                dataset,
                pg_checkpoint.as_uri(),
                hdfs_checkpoint,
                hdfs_output,
            )
            try:
                kafka_events = (
                    spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", args.bootstrap_servers)
                    .option("subscribe", args.topic)
                    .option("startingOffsets", args.starting_offsets)
                    .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
                    .option("failOnDataLoss", "true")
                    .load()
                )
            except Exception as exc:
                if "failed to find data source: kafka" in str(exc).lower():
                    raise RuntimeError(
                        f"Spark could not load Kafka data source. Required connector: "
                        f"{KAFKA_CONNECTOR}. It was configured before SparkSession "
                        "creation; check the startup logs for Maven/Ivy download errors."
                    ) from exc
                raise
            aggregates = build_aggregates(
                kafka_events,
                args.threshold,
                dataset=dataset,
                watermark_delay=args.watermark_delay,
            )

            def persist_dataset_batch(
                batch_df: Any, batch_id: int, checkpoint: Path = pg_checkpoint
            ) -> None:
                persist_batch(batch_df, batch_id, checkpoint)

            # Spark 4.0 supports the chained stateful operators in Append mode.
            # The PostgreSQL upsert remains idempotent for finalized windows.
            postgres_query = (
                aggregates.writeStream.outputMode("append")
                .foreachBatch(persist_dataset_batch)
                .option("checkpointLocation", pg_checkpoint.as_uri())
                .queryName(f"traffic_metrics_{dataset.replace('-', '_')}")
                .start()
            )
            queries.append(postgres_query)
            readiness.register_query(
                dataset,
                "postgres",
                postgres_query,
                expected_source=args.topic,
                expected_sink="ForeachBatchSink",
            )
            LOGGER.info(
                "PostgreSQL query started for %s (id=%s, checkpoint=%s)",
                dataset,
                postgres_query.id,
                pg_checkpoint.as_uri(),
            )

            try:
                hdfs_query = (
                    aggregates.writeStream.format("parquet")
                    .outputMode("append")
                    .option("path", hdfs_output)
                    .option("checkpointLocation", hdfs_checkpoint)
                    .partitionBy("dataset")
                    .queryName(f"traffic_metrics_hdfs_{dataset.replace('-', '_')}")
                    .start()
                )
            except Exception as exc:
                raise RuntimeError(
                    "Could not start the HDFS Parquet query. Check Spark's Hadoop "
                    f"configuration, HDFS reachability, and path permissions. "
                    f"Output: {hdfs_output}; checkpoint: {hdfs_checkpoint}. "
                    f"Original error: {exc}"
                ) from exc
            queries.append(hdfs_query)
            readiness.register_query(
                dataset,
                "hdfs",
                hdfs_query,
                expected_source=args.topic,
                expected_sink=hdfs_output,
            )
            LOGGER.info(
                "HDFS query started for %s (id=%s, output=%s, checkpoint=%s)",
                dataset,
                hdfs_query.id,
                hdfs_output,
                hdfs_checkpoint,
            )

            marker_checkpoint = checkpoint_path(args.checkpoint_dir, dataset, "replay_ledger")
            marker_stream = build_replay_marker_stream(kafka_events, dataset)

            def persist_dataset_markers(
                batch_df: Any, batch_id: int, checkpoint: Path = pg_checkpoint
            ) -> None:
                persist_replay_markers(batch_df, batch_id, checkpoint)

            marker_query = (
                marker_stream.writeStream.outputMode("append")
                .foreachBatch(persist_dataset_markers)
                .option("checkpointLocation", marker_checkpoint.as_uri())
                .queryName(f"traffic_replay_ledger_{dataset.replace('-', '_')}")
                .start()
            )
            queries.append(marker_query)
            readiness.register_query(
                dataset,
                "replay_ledger",
                marker_query,
                expected_source=args.topic,
                expected_sink="ForeachBatchSink",
            )
            LOGGER.info(
                "Replay marker ledger query started for %s (id=%s, checkpoint=%s)",
                dataset,
                marker_query.id,
                marker_checkpoint.as_uri(),
            )

        try:
            spark.streams.awaitAnyTermination()
        except Exception as exc:
            details = query_failure_messages(queries)
            detail = "; ".join(details) if details else str(exc)
            raise RuntimeError(f"A Spark streaming query failed: {detail}") from exc

        ended = [query.name for query in queries if not query.isActive]
        if ended:
            details = query_failure_messages(queries)
            detail = "; ".join(details) if details else ", ".join(ended)
            raise RuntimeError(f"A Spark streaming query terminated unexpectedly: {detail}")
    except KeyboardInterrupt:
        LOGGER.info("Shutdown requested; stopping both streaming queries")
    finally:
        if readiness_server is not None:
            try:
                readiness_server.shutdown()
                readiness_server.server_close()
            except Exception:
                LOGGER.exception("Could not stop readiness endpoint")
        for query in reversed(queries):
            try:
                if query.isActive:
                    query.stop()
            except Exception:
                LOGGER.exception("Could not stop streaming query %s cleanly", query.name)
        try:
            spark.stop()
        except Exception:
            LOGGER.exception("Could not stop Spark cleanly")
        LOGGER.info("Spark streaming job stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        return run(args)
    except (RuntimeError, ValueError) as exc:
        LOGGER.error("Could not start traffic stream: %s", exc)
        return 2
    except Exception:
        LOGGER.exception("Traffic stream failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
