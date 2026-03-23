"""JSON-backed connector registry with versioning."""
import json
import yaml
from datetime import datetime
from pathlib import Path
from typing import Optional

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
REGISTRY_FILE = DATA_DIR / "registry.json"


class ConnectorRegistry:
    def __init__(self, registry_file: Path = REGISTRY_FILE):
        self.registry_file = registry_file
        self._registry = self._load()

    def _load(self) -> dict:
        """Load registry from disk, returning an empty registry on failure."""
        if self.registry_file.exists():
            try:
                with open(self.registry_file) as f:
                    return json.load(f)
            except Exception:
                pass
        return {"connectors": {}}

    def _save(self):
        with open(self.registry_file, "w") as f:
            json.dump(self._registry, f, indent=2, default=str)

    def _next_version(self, name: str) -> str:
        """Increment the patch segment of the connector's current semver string."""
        if name not in self._registry["connectors"]:
            return "1.0.0"
        current = self._registry["connectors"][name].get("current_version", "1.0.0")
        parts = current.split(".")
        parts[2] = str(int(parts[2]) + 1)
        return ".".join(parts)

    def save_connector(
        self,
        name: str,
        manifest: dict,
        status: str = "sandbox",
        field_accuracy: float = 0.95,
        fingerprint: str = None,
        filled_fields: list = None,
        connector_docs: dict = None,
    ) -> str:
        version = self._next_version(name)
        try:
            manifest_yaml = yaml.dump(manifest, default_flow_style=False, allow_unicode=True)
        except Exception:
            manifest_yaml = json.dumps(manifest, indent=2)

        connector_version = {
            "version": version,
            "created_at": datetime.utcnow().isoformat(),
            "status": status,
            "manifest_yaml": manifest_yaml,
            "manifest_dict": manifest,
            "field_accuracy": field_accuracy,
            "validation_passed": True,
            "schema_fingerprint": fingerprint,
            "filled_fields": filled_fields or [],
            "connector_docs": connector_docs or {},
        }

        if name not in self._registry["connectors"]:
            self._registry["connectors"][name] = {
                "name": name,
                "connector_type": manifest.get("connector_type", "rest"),
                "auth_type": (manifest.get("auth") or {}).get("type", "unknown"),
                "current_version": version,
                "versions": [connector_version],
                "last_validated": datetime.utcnow().isoformat(),
                "total_syncs": 0,
                "created_at": datetime.utcnow().isoformat(),
            }
        else:
            existing = self._registry["connectors"][name]
            existing["current_version"] = version
            existing["last_validated"] = datetime.utcnow().isoformat()
            existing["versions"].append(connector_version)

        self._save()
        return version

    def get_connector(self, name: str) -> Optional[dict]:
        return self._registry["connectors"].get(name)

    def list_connectors(self) -> list:
        return list(self._registry["connectors"].values())

    def promote_to_production(self, name: str):
        """Set the latest version of a connector to 'production' status."""
        if name in self._registry["connectors"]:
            connector = self._registry["connectors"][name]
            if connector.get("versions"):
                connector["versions"][-1]["status"] = "production"
            self._save()

    def demote_to_sandbox(self, name: str):
        """Revert the latest version of a connector back to 'sandbox' status."""
        if name in self._registry["connectors"]:
            connector = self._registry["connectors"][name]
            if connector.get("versions"):
                connector["versions"][-1]["status"] = "sandbox"
            self._save()

    def delete_connector(self, name: str) -> bool:
        if name in self._registry["connectors"]:
            del self._registry["connectors"][name]
            self._save()
            return True
        return False

    def increment_sync_count(self, name: str, records: int = 0):
        """Record a completed sync run — increments total_syncs and total_records."""
        if name in self._registry["connectors"]:
            self._registry["connectors"][name]["total_syncs"] += 1
            self._registry["connectors"][name].setdefault("total_records", 0)
            self._registry["connectors"][name]["total_records"] += records
            self._save()

    def get_current_manifest(self, name: str) -> Optional[dict]:
        connector = self.get_connector(name)
        if connector and connector["versions"]:
            return connector["versions"][-1].get("manifest_dict")
        return None

    def get_current_version_info(self, name: str) -> Optional[dict]:
        connector = self.get_connector(name)
        if connector and connector["versions"]:
            return connector["versions"][-1]
        return None

    def stats(self) -> dict:
        connectors = self.list_connectors()
        total = len(connectors)
        production = sum(
            1 for c in connectors
            if c.get("versions") and c["versions"][-1].get("status") == "production"
        )
        sandbox = total - production
        accuracies = [
            c["versions"][-1].get("field_accuracy", 0)
            for c in connectors if c.get("versions")
        ]
        avg_accuracy = sum(accuracies) / len(accuracies) if accuracies else 0
        return {
            "total": total,
            "production": production,
            "sandbox": sandbox,
            "avg_accuracy": avg_accuracy,
        }
