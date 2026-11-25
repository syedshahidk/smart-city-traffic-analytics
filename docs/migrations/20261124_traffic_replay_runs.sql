-- Additive replay completion ledger for the Spark v2 replay workflow.
-- Apply manually during a controlled deployment; this file is not run by the app.
-- Rollback: only after stopping replay writers and exporting any audit records,
-- DROP TABLE public.traffic_replay_runs; this removes the ledger history.

BEGIN;

CREATE TABLE public.traffic_replay_runs (
    replay_id UUID PRIMARY KEY,
    dataset TEXT NOT NULL CHECK (dataset IN ('metr-la', 'pems-bay')),
    expected_last_source_timestamp TIMESTAMPTZ NOT NULL,
    expected_final_window_start TIMESTAMPTZ NOT NULL,
    expected_final_window_end TIMESTAMPTZ NOT NULL,
    expected_average_speed DOUBLE PRECISION,
    expected_total_sensor_readings BIGINT,
    expected_congested_sensor_count BIGINT,
    expected_congestion_percentage DOUBLE PRECISION,
    marker_topic TEXT,
    marker_partition INTEGER,
    marker_offset BIGINT,
    marker_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (marker_status IN ('pending', 'observed')),
    producer_ack_topic TEXT,
    producer_ack_partition INTEGER,
    producer_ack_offset BIGINT,
    producer_acknowledged_at TIMESTAMPTZ,
    postgres_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (postgres_status IN ('pending', 'verified')),
    postgres_batch_id BIGINT,
    postgres_verified_at TIMESTAMPTZ,
    hdfs_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (hdfs_status IN ('pending', 'verified')),
    hdfs_batch_id BIGINT,
    hdfs_verified_at TIMESTAMPTZ,
    overall_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (overall_status IN ('pending', 'complete', 'failed')),
    status_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_traffic_replay_window
        CHECK (expected_final_window_end > expected_final_window_start
               AND expected_final_window_end - expected_final_window_start = INTERVAL '5 minutes'),
    CONSTRAINT ck_traffic_replay_expected_counts
        CHECK ((expected_total_sensor_readings IS NULL OR expected_total_sensor_readings >= 0)
               AND (expected_congested_sensor_count IS NULL OR expected_congested_sensor_count >= 0)
               AND (expected_congested_sensor_count IS NULL OR expected_total_sensor_readings IS NULL
                    OR expected_congested_sensor_count <= expected_total_sensor_readings)),
    CONSTRAINT ck_traffic_replay_marker_position
        CHECK ((marker_status = 'pending' AND marker_topic IS NULL
                AND marker_partition IS NULL AND marker_offset IS NULL)
               OR (marker_status = 'observed' AND marker_topic IS NOT NULL
                   AND marker_partition IS NOT NULL AND marker_partition >= 0
                   AND marker_offset IS NOT NULL AND marker_offset >= 0)),
    CONSTRAINT ck_traffic_replay_postgres_verification
        CHECK (postgres_status <> 'verified' OR
               (postgres_batch_id IS NOT NULL AND postgres_verified_at IS NOT NULL)),
    CONSTRAINT ck_traffic_replay_hdfs_verification
        CHECK (hdfs_status <> 'verified' OR
               (hdfs_batch_id IS NOT NULL AND hdfs_verified_at IS NOT NULL)),
    CONSTRAINT ck_traffic_replay_producer_ack
        CHECK ((producer_ack_topic IS NULL AND producer_ack_partition IS NULL
                AND producer_ack_offset IS NULL AND producer_acknowledged_at IS NULL)
               OR (producer_ack_topic IS NOT NULL AND producer_ack_partition IS NOT NULL
                   AND producer_ack_partition >= 0 AND producer_ack_offset IS NOT NULL
                   AND producer_ack_offset >= 0 AND producer_acknowledged_at IS NOT NULL)),
    CONSTRAINT ck_traffic_replay_completion
        CHECK (overall_status <> 'complete' OR
               (marker_status = 'observed' AND postgres_status = 'verified' AND hdfs_status = 'verified'))
);

CREATE INDEX ix_traffic_replay_runs_pending
    ON public.traffic_replay_runs (dataset, created_at)
    WHERE overall_status = 'pending';

CREATE INDEX ix_traffic_replay_runs_marker
    ON public.traffic_replay_runs (marker_topic, marker_partition, marker_offset)
    WHERE marker_status = 'pending';

COMMIT;
