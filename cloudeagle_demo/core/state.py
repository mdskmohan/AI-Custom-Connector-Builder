"""State management: JSON file backed, SQLite for production."""
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"


@dataclass
class SyncRun:
    connector_name: str
    stream_name: str
    started_at: str
    status: str  # running, completed, failed
    records_synced: int = 0
    pages_fetched: int = 0
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    error: Optional[str] = None
    last_cursor: Optional[str] = None
    partitions_total: int = 0
    partitions_done: int = 0


class StateManager:
    def __init__(self, state_file: Path = STATE_FILE):
        self.state_file = state_file
        self._state = self._load()

    def _load(self) -> dict:
        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    return json.load(f)
            except Exception:
                pass
        return {"checkpoints": {}, "sync_history": [], "last_requests": {}}

    def _save(self):
        with open(self.state_file, "w") as f:
            json.dump(self._state, f, indent=2, default=str)

    def get_checkpoint(self, connector_name: str, stream_name: str) -> Optional[dict]:
        key = f"{connector_name}:{stream_name}"
        return self._state["checkpoints"].get(key)

    def save_checkpoint(self, connector_name: str, stream_name: str, checkpoint: dict):
        key = f"{connector_name}:{stream_name}"
        self._state["checkpoints"][key] = {
            **checkpoint,
            "saved_at": datetime.utcnow().isoformat(),
        }
        self._save()

    def save_last_request(self, connector_name: str, stream_name: str, data: dict):
        key = f"{connector_name}:{stream_name}"
        if "last_requests" not in self._state:
            self._state["last_requests"] = {}
        self._state["last_requests"][key] = {**data, "captured_at": datetime.utcnow().isoformat()}
        self._save()

    def get_last_request(self, connector_name: str, stream_name: str) -> Optional[dict]:
        key = f"{connector_name}:{stream_name}"
        return self._state.get("last_requests", {}).get(key)

    def add_sync_run(self, run: SyncRun):
        if not isinstance(self._state.get("sync_history"), list):
            self._state["sync_history"] = []
        self._state["sync_history"].append(asdict(run))
        self._state["sync_history"] = self._state["sync_history"][-100:]
        self._save()

    def get_sync_history(self, connector_name: str = None) -> list:
        history = self._state["sync_history"]
        if connector_name:
            history = [h for h in history if h["connector_name"] == connector_name]
        return list(reversed(history))

    def update_sync_run(self, connector_name: str, started_at: str, updates: dict):
        for run in self._state["sync_history"]:
            if run["connector_name"] == connector_name and run["started_at"] == started_at:
                run.update(updates)
                break
        self._save()

    def clear_checkpoint(self, connector_name: str, stream_name: str):
        key = f"{connector_name}:{stream_name}"
        self._state["checkpoints"].pop(key, None)
        self._save()
