"""Health signal collection and issue classifier."""
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from .state import StateManager


@dataclass
class HealthSignal:
    connector_name: str
    stream_name: str
    timestamp: str
    total_records: int
    null_rate_cursor: float
    error_rate: float
    p95_latency_ms: float
    pagination_advanced: bool
    issue_type: Optional[str] = None  # config, non_config, None
    issue_detail: Optional[str] = None


def _compute_null_rate(records: list, cursor_field: Optional[str]) -> float:
    if not records or not cursor_field:
        return 0.0
    null_count = sum(1 for r in records if r.get(cursor_field) is None)
    return null_count / len(records)


class IssueClassifier:
    """Classify issues as config (manifest error) or non-config (infra error)."""

    CONFIG_ISSUES = {
        "null_cursor_rate_high": "Null rate on cursor_field > 5% — likely wrong cursor_field path in manifest",
        "zero_records": "Zero records every sync — likely wrong record_selector path",
        "schema_mismatch": "Schema fingerprint changed — new fields in API response",
        "wrong_primary_key": "Primary key null in records — wrong primary_key field name",
    }

    NON_CONFIG_ISSUES = {
        "connection_timeout": "Connection timeout — check network connectivity",
        "auth_401": "401 Unauthorized — check API key or token validity",
        "rate_limited": "Rate limit exceeded — reduce sync frequency",
        "server_error": "5xx Server Error — check API status page",
    }

    def classify(self, signal: HealthSignal) -> tuple:
        """Returns (issue_type, issue_key, action) or (None, None, None)."""
        if signal.null_rate_cursor > 0.05:
            return "config", "null_cursor_rate_high", "Re-generate cursor_field via feedback loop"
        if signal.total_records == 0:
            return "config", "zero_records", "Re-generate record_selector via feedback loop"
        if signal.error_rate > 0.1:
            if signal.error_rate > 0.5:
                return "non_config", "connection_timeout", "Ops alert — check network"
        return None, None, None


class HealthCollector:
    def __init__(self):
        self._state = StateManager()

    def collect_from_sync_history(self, connector_name: str) -> list:
        """Build health signals from sync history."""
        history = self._state.get_sync_history(connector_name)
        signals = []

        for run in history[:10]:
            records = run.get("records_synced", 0)
            duration = run.get("duration_seconds", 1) or 1
            error = run.get("error")

            signal = HealthSignal(
                connector_name=connector_name,
                stream_name=run.get("stream_name", "unknown"),
                timestamp=run.get("started_at", datetime.utcnow().isoformat()),
                total_records=records,
                null_rate_cursor=0.0,
                error_rate=1.0 if error else 0.0,
                p95_latency_ms=(duration * 1000) * 0.95,
                pagination_advanced=run.get("pages_fetched", 0) > 1,
                issue_type="non_config" if error else None,
                issue_detail=error,
            )
            signals.append(signal)

        return signals

    def compute_summary(self, connector_name: str) -> dict:
        """Compute aggregate health metrics."""
        history = self._state.get_sync_history(connector_name)

        if not history:
            return {
                "total_records": 0,
                "null_rate": 0.0,
                "error_rate": 0.0,
                "p95_latency_ms": 0.0,
                "pagination_advanced": False,
                "sync_count": 0,
            }

        total_records = sum(h.get("records_synced", 0) for h in history)
        errors = sum(1 for h in history if h.get("error"))
        durations = [h.get("duration_seconds", 0) for h in history if h.get("duration_seconds")]
        p95 = sorted(durations)[int(len(durations) * 0.95)] * 1000 if durations else 0

        return {
            "total_records": total_records,
            "null_rate": 0.02,  # Simulated
            "error_rate": errors / len(history),
            "p95_latency_ms": p95,
            "pagination_advanced": any(h.get("pages_fetched", 0) > 1 for h in history),
            "sync_count": len(history),
        }

    def compute_schema_fingerprint(self, records: list) -> str:
        if not records:
            return ""
        sample = records[0]
        field_info = sorted([(k, type(v).__name__) for k, v in sample.items()])
        return hashlib.sha256(json.dumps(field_info).encode()).hexdigest()[:16]
