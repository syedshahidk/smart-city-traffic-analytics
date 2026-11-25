"""HTTP readiness signal shared by the Spark service and replay producer.

Readiness is based on active Spark query objects and a successful progress
record from each query's current run. Persisted checkpoints alone cannot make a
new Spark process ready.
"""

from __future__ import annotations

from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Thread
from typing import Any
from urllib.parse import parse_qs, urlparse


SUPPORTED_DATASETS = frozenset(("metr-la", "pems-bay"))
SUPPORTED_SINKS = frozenset(("postgres", "hdfs", "replay_ledger"))


@dataclass(frozen=True)
class SparkReady:
    """Proof that the requested dataset queries initialized in this process."""

    datasets: frozenset[str]
    watermark_delay: str
    generation: str


class ReadinessState:
    """Track the active Postgres and HDFS queries for each selected dataset."""

    def __init__(self, datasets: set[str] | frozenset[str], watermark_delay: str, generation: str):
        if not datasets or not datasets <= SUPPORTED_DATASETS:
            raise ValueError("readiness requires one or more supported datasets")
        self.datasets = frozenset(datasets)
        self.watermark_delay = watermark_delay
        self.generation = generation
        self._queries: dict[tuple[str, str], tuple[Any, str, str]] = {}

    def register_query(
        self,
        dataset: str,
        sink: str,
        query: Any,
        *,
        expected_source: str,
        expected_sink: str,
    ) -> None:
        if dataset not in self.datasets or sink not in SUPPORTED_SINKS:
            raise ValueError("query does not belong to this readiness service")
        self._queries[(dataset, sink)] = (query, expected_source, expected_sink)

    @staticmethod
    def _diagnose_query(
        query: Any, expected_source: str, expected_sink: str
    ) -> dict[str, Any]:
        diagnostic: dict[str, Any] = {
            "state": "starting",
            "reason": None,
            "query_id": None,
            "run_id": None,
            "batch_id": None,
            "source_confirmed": False,
            "sink_confirmed": False,
            "initial_offsets_confirmed": False,
        }
        if query is None:
            diagnostic.update(state="not_started", reason="query has not been registered")
            return diagnostic
        try:
            diagnostic["query_id"] = str(query.id)
            diagnostic["run_id"] = str(query.runId)
            if not query.isActive:
                error = query.exception()
                diagnostic.update(
                    state="failed" if error is not None else "inactive",
                    reason=str(error) if error is not None else "query is not active",
                )
                return diagnostic
            error = query.exception()
            if error is not None:
                diagnostic.update(state="failed", reason=str(error))
                return diagnostic
            progress = query.lastProgress
            if progress is None:
                # A fresh query at Kafka `latest` may have no batch to commit.
                # Spark reports these idle messages only after its trigger has
                # initialized the source and polled offsets. Do not infer readiness
                # from active/isActive or persisted checkpoint files alone.
                status = query.status
                message = str(status.get("message", "")) if isinstance(status, dict) else ""
                if message not in {"Waiting for data to arrive", "Waiting for next trigger"}:
                    diagnostic.update(
                        state="awaiting_progress",
                        reason="current query run has not initialized and polled its source",
                    )
                    return diagnostic
                diagnostic.update(
                    state="ready",
                    reason="current query run initialized and polled Kafka; no data was available yet",
                    source_confirmed=True,
                    sink_confirmed=True,
                    initial_offsets_confirmed=True,
                    sink_description=expected_sink,
                )
                return diagnostic

            progress_query_id = str(progress.get("id", ""))
            progress_run_id = str(progress.get("runId", ""))
            if progress_query_id != diagnostic["query_id"]:
                diagnostic.update(state="stale_progress", reason="progress belongs to another query")
                return diagnostic
            if progress_run_id != diagnostic["run_id"]:
                diagnostic.update(
                    state="awaiting_progress",
                    reason="no completed progress is available for the current query run",
                )
                return diagnostic

            sources = progress.get("sources") or []
            source_confirmed = any(
                expected_source in str(source.get("description", ""))
                or expected_source in str(source.get("endOffset", ""))
                for source in sources
            )
            diagnostic["source_confirmed"] = source_confirmed
            if not source_confirmed:
                diagnostic.update(
                    state="source_mismatch",
                    reason=f"progress does not identify Kafka source {expected_source!r}",
                )
                return diagnostic

            sink_progress = progress.get("sink") or {}
            sink_description = str(sink_progress.get("description", ""))
            sink_confirmed = expected_sink.casefold() in sink_description.casefold()
            diagnostic["sink_confirmed"] = sink_confirmed
            diagnostic["batch_id"] = progress.get("batchId")
            diagnostic["sink_description"] = sink_description
            if not sink_confirmed:
                diagnostic.update(
                    state="sink_mismatch",
                    reason=f"progress sink {sink_description!r} does not match {expected_sink!r}",
                )
                return diagnostic

            diagnostic.update(state="ready", reason="current run committed a matching source/sink batch")
            return diagnostic
        except Exception as exc:
            diagnostic.update(state="diagnostic_error", reason=f"could not inspect query: {exc}")
            return diagnostic

    def ready_datasets(self) -> set[str]:
        diagnostics = self.diagnostics()
        return {
            dataset
            for dataset in self.datasets
            if all(
                diagnostics[dataset][sink]["state"] == "ready"
                for sink in SUPPORTED_SINKS
            )
        }

    def diagnostics(self) -> dict[str, dict[str, dict[str, Any]]]:
        return {
            dataset: {
                sink: self._diagnose_query(*self._queries[(dataset, sink)])
                if (dataset, sink) in self._queries
                else self._diagnose_query(None, "", "")
                for sink in sorted(SUPPORTED_SINKS)
            }
            for dataset in sorted(self.datasets)
        }

    def response(self, requested: set[str]) -> tuple[int, dict[str, Any]]:
        if not requested or not requested <= self.datasets:
            return 400, {"status": "error", "message": "unsupported or unconfigured dataset"}
        ready = self.ready_datasets()
        if requested <= ready:
            return 200, {
                "status": "ready",
                "ready_datasets": sorted(ready),
                "watermark_delay": self.watermark_delay,
                "generation": self.generation,
                "diagnostics": self.diagnostics(),
            }
        return 503, {
            "status": "starting",
            "ready_datasets": sorted(ready),
            "watermark_delay": self.watermark_delay,
            "generation": self.generation,
            "diagnostics": self.diagnostics(),
        }


def start_readiness_server(
    state: ReadinessState, host: str = "0.0.0.0", port: int = 8081,
    replay_api: Any | None = None,
) -> tuple[ThreadingHTTPServer, Thread]:
    """Start the local health endpoint; producers should use /health/ready."""

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
            parsed = urlparse(self.path)
            if replay_api is None or not (parsed.path == "/replays" or parsed.path.startswith("/replays/") and parsed.path.endswith("/marker")):
                status, payload = 404, {"status": "error", "message": "not found"}
            else:
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if length <= 0 or length > 64 * 1024:
                        raise ValueError("request body must be between 1 and 65536 bytes")
                    body = json.loads(self.rfile.read(length).decode("utf-8"))
                    if not isinstance(body, dict):
                        raise ValueError("replay manifest must be a JSON object")
                    if parsed.path == "/replays":
                        payload = replay_api.register(body)
                        status = 201
                    else:
                        replay_id = parsed.path.removeprefix("/replays/").removesuffix("/marker")
                        result = replay_api.acknowledge(
                            replay_id, str(body["topic"]), int(body["partition"]), int(body["offset"])
                        )
                        status = 200 if result["status"] == "acknowledged" else 409
                        payload = result
                except (ValueError, KeyError, TypeError) as exc:
                    status, payload = 400, {"status": "error", "message": str(exc)}
                except Exception:
                    status, payload = 503, {
                        "status": "error",
                        "message": "replay ledger is unavailable",
                    }
            encoded = json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
            parsed = urlparse(self.path)
            if parsed.path.startswith("/replays/"):
                replay_id = parsed.path.removeprefix("/replays/")
                if replay_api is None or not replay_id or "/" in replay_id:
                    status, payload = 404, {"status": "error", "message": "not found"}
                else:
                    try:
                        payload = replay_api.get(replay_id)
                        status = 200 if payload is not None else 404
                        if payload is None:
                            payload = {"status": "error", "message": "replay not found"}
                    except Exception:
                        status, payload = 503, {
                            "status": "error",
                            "message": "replay ledger is unavailable",
                        }
                encoded = json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                return

            # Health routes retain their existing behavior.
            if parsed.path == "/health/live":
                status, payload = 200, {"status": "live"}
            elif parsed.path == "/health/ready":
                raw = parse_qs(parsed.query).get("datasets", [])
                requested = set(filter(None, ",".join(raw).split(",")))
                if not requested:
                    requested = set(state.datasets)
                status, payload = state.response(requested)
            else:
                status, payload = 404, {"status": "error", "message": "not found"}
            encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: Any) -> None:
            return

    server = ThreadingHTTPServer((host, port), Handler)
    thread = Thread(target=server.serve_forever, name="spark-readiness-http", daemon=True)
    thread.start()
    return server, thread
