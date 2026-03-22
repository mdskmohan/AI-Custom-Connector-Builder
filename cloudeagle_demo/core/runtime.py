"""Runtime executor: auth, paginate, checkpoint, write to SQLite."""
import re
import sqlite3
import time
import json
import requests
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable

from .state import StateManager, SyncRun

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "destination.db"


@dataclass
class SyncResult:
    connector_name: str
    stream_name: str
    records_synced: int
    pages_fetched: int
    duration_seconds: float
    status: str  # completed, failed
    error: Optional[str] = None
    partitions_completed: int = 0
    health_signals: dict = field(default_factory=dict)


def _apply_selector(data, selector: str) -> list:
    """Apply simple JSONPath selector."""
    if not selector:
        return [data] if data else []
    if selector in ("$[*]", "$.*"):
        if isinstance(data, list):
            return data
        # Dict response — try common list keys before giving up
        if isinstance(data, dict):
            for key in ("list", "data", "items", "results", "records", "entries", "values"):
                if isinstance(data.get(key), list):
                    return data[key]
        return []
    if selector.startswith("$."):
        keys = selector[2:].split(".")
        result = data
        for key in keys:
            if isinstance(result, dict):
                result = result.get(key)
            else:
                return []
        if isinstance(result, list):
            return result
        elif result is not None:
            return [result]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _infer_sql_type(value) -> str:
    if isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    return "TEXT"


class SyncRuntime:
    def __init__(
        self,
        manifest: dict,
        credential: str = "",
        db_path: Path = DB_PATH,
        log_callback: Optional[Callable] = None,
        progress_callback: Optional[Callable] = None,
    ):
        self.manifest = manifest
        self.credential = credential
        self.db_path = db_path
        self.log = log_callback or (lambda msg, level="info": None)
        self.progress = progress_callback or (lambda **kwargs: None)
        self._state = StateManager()
        self._conn = None

    def _get_conn(self):
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _build_headers(self) -> dict:
        auth = self.manifest.get("auth") or {}
        auth_type = auth.get("type", "none")
        headers = {"User-Agent": "CloudEagle-Runtime/1.0", "Accept": "application/json"}

        base_url = self.manifest.get("base_url", "")
        if "github.com" in base_url:
            headers.update({
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            })

        if not self.credential or auth_type == "none":
            return headers

        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self.credential}"
        elif auth_type == "api_key" and auth.get("in", "header") != "query":
            # query-param auth is injected per-request via _build_auth_params()
            header_name = auth.get("header_name", "X-API-Key")
            headers[header_name] = self.credential
        return headers

    def _build_auth_params(self) -> dict:
        """Return query params that carry the credential (e.g. ?appid=... for OpenWeatherMap)."""
        auth = self.manifest.get("auth") or {}
        if auth.get("type") == "api_key" and auth.get("in") == "query" and self.credential:
            return {auth.get("header_name", "appid"): self.credential}
        return {}

    def _resolve_calendly_context(self, headers: dict) -> dict:
        """
        Calendly streams require organization/user URIs from /users/me.
        Fetch once per runtime instance and cache the result.
        Returns dict with 'user_uri' and 'org_uri', or empty dict on failure.
        """
        if hasattr(self, "_calendly_ctx"):
            return self._calendly_ctx
        base_url = self.manifest.get("base_url", "").rstrip("/")
        try:
            r = requests.get(f"{base_url}/users/me", headers=headers, timeout=10)
            if r.status_code == 200:
                resource = r.json().get("resource", {})
                self._calendly_ctx = {
                    "user_uri":    resource.get("uri", ""),
                    "org_uri":     resource.get("current_organization", ""),
                    "user_name":   resource.get("name", ""),
                    "user_email":  resource.get("email", ""),
                }
                self.log(
                    f"Calendly context resolved: user={self._calendly_ctx['user_name']} "
                    f"org={self._calendly_ctx['org_uri'][:40]}",
                    "info",
                )
                return self._calendly_ctx
            else:
                self.log(f"Could not resolve Calendly context: HTTP {r.status_code}", "warning")
        except Exception as e:
            self.log(f"Calendly context fetch failed: {e}", "warning")
        self._calendly_ctx = {}
        return {}

    def _inject_calendly_params(self, stream: dict, base_params: dict, headers: dict) -> dict:
        """
        Inject required Calendly params (organization, user) into stream params.
        Without these, Calendly returns HTTP 400.
        """
        stream_name = stream.get("name", "")
        ctx = self._resolve_calendly_context(headers)
        params = dict(base_params)
        org_uri  = ctx.get("org_uri", "")
        user_uri = ctx.get("user_uri", "")

        if stream_name == "scheduled_events" and org_uri:
            params["organization"] = org_uri
        if stream_name == "event_types" and user_uri:
            params["user"] = user_uri
        return params


    def _create_table(self, stream_name: str, record: dict, connector_name: str):
        table = f"{connector_name}_{stream_name}".replace("-", "_").lower()
        columns = []
        for k, v in record.items():
            col_type = _infer_sql_type(v)
            col_name = re.sub(r"[^a-zA-Z0-9_]", "_", str(k))
            columns.append(f'"{col_name}" {col_type}')
        if columns:
            ddl = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(columns)})'
            try:
                self._get_conn().execute(ddl)
                self._get_conn().commit()
            except Exception as e:
                self.log(f"Table creation error: {e}", "warning")
        return table

    def _insert_records(self, table: str, records: list):
        if not records:
            return
        try:
            conn = self._get_conn()
            for record in records:
                cols = [re.sub(r"[^a-zA-Z0-9_]", "_", str(k)) for k in record.keys()]
                vals = []
                for v in record.values():
                    if isinstance(v, (dict, list)):
                        vals.append(json.dumps(v))
                    else:
                        vals.append(v)
                placeholders = ", ".join("?" * len(cols))
                col_names = ", ".join(f'"{c}"' for c in cols)
                sql = f'INSERT OR REPLACE INTO "{table}" ({col_names}) VALUES ({placeholders})'
                try:
                    conn.execute(sql, vals)
                except Exception:
                    pass
            conn.commit()
        except Exception as e:
            self.log(f"Insert error: {e}", "warning")

    def _redact_headers(self, headers: dict) -> dict:
        """Return headers with credential values replaced by [REDACTED]."""
        sensitive = {"authorization", "x-api-key", "api-key"}
        return {k: "[REDACTED]" if k.lower() in sensitive else v for k, v in headers.items()}

    def _fetch_page(self, stream: dict, params: dict, headers: dict, cursor: str = None) -> tuple:
        """Returns (records, next_cursor)."""
        base_url = self.manifest.get("base_url", "").rstrip("/")
        path = stream.get("path", "")
        if path and not path.startswith("/"):
            path = "/" + path
        url = base_url + path
        selector = stream.get("record_selector", "$[*]")
        pagination = stream.get("pagination") or {}
        strategy = pagination.get("strategy", "none")

        method = (stream.get("method") or "GET").upper()
        auth_query_params = self._build_auth_params()
        # Auth query params always go in the URL; static stream params go in body for POST
        if cursor and strategy == "cursor":
            cursor_param = pagination.get("cursor_param", "page_token")
            auth_query_params[cursor_param] = cursor
        # Merge static stream params + auth query params for GET; keep separate for POST
        get_params = {**dict(params), **auth_query_params}
        post_body  = {**dict(params)}  # no auth creds in POST body

        def _do_request():
            if method == "POST":
                return requests.post(url, headers=headers, params=auth_query_params, json=post_body, timeout=10)
            else:  # GET (only GET/POST supported for data retrieval)
                return requests.get(url, headers=headers, params=get_params, timeout=10)

        try:
            r = _do_request()

            # ── Capture request + response for debug panel ─────────────────
            try:
                resp_body = r.text[:4000]
                try:
                    resp_json = r.json()
                except Exception:
                    resp_json = None
                debug_data = {
                    "request": {
                        "method": method,
                        "url": r.url,
                        "headers": self._redact_headers(dict(headers)),
                        "params": {k: "[REDACTED]" if k.lower() in {"appid", "api_key", "apikey", "key", "token"} else v
                                   for k, v in get_params.items()},
                    },
                    "response": {
                        "status_code": r.status_code,
                        "headers": dict(r.headers),
                        "body": resp_json if resp_json is not None else resp_body,
                    },
                }
                stream_name = stream.get("name", "unknown")
                connector_name = getattr(self, "_current_connector_name", None) or self.manifest.get("name", "unknown")
                self._state.save_last_request(connector_name, stream_name, debug_data)
            except Exception:
                pass  # never break sync due to debug capture

            if r.status_code == 429:
                reset_ts = r.headers.get("X-RateLimit-Reset") or r.headers.get("Retry-After")
                wait = 10
                if reset_ts:
                    try:
                        reset = float(reset_ts)
                        wait = max(1, int(reset - datetime.utcnow().timestamp()) + 1)
                    except Exception:
                        pass
                wait = min(wait, 30)  # cap at 30s for demo
                self.log(f"Rate limited — waiting {wait}s (X-RateLimit-Reset)", "warning")
                time.sleep(wait)
                r = _do_request()

            if r.status_code == 204:
                return [], None  # No content — stream exhausted
            if r.status_code != 200:
                self.log(f"HTTP {r.status_code} from {url}", "error")
                return [], None

            data = r.json()
            records = _apply_selector(data, selector)

            # Extract next cursor
            next_cursor = None
            if strategy == "link_header":
                link = r.headers.get("Link", "")
                match = re.search(r'<([^>]+)>;\s*rel="next"', link)
                if match:
                    next_cursor = match.group(1)  # full URL for next page
            elif strategy == "cursor":
                cursor_path = pagination.get("cursor_field", "")
                if cursor_path.startswith("$."):
                    keys = cursor_path[2:].split(".")
                    val = data
                    for k in keys:
                        val = (val or {}).get(k) if isinstance(val, dict) else None
                    next_cursor = val

            return records, next_cursor

        except Exception as e:
            self.log(f"Fetch error: {e}", "error")
            return [], None

    def run_stream(
        self,
        stream: dict,
        connector_name: str,
        sync_mode: str = "full_refresh",
    ) -> SyncResult:
        self._current_connector_name = connector_name  # used by _fetch_page for debug capture
        stream_name = stream.get("name", "unknown")
        started_at = datetime.utcnow().isoformat()
        t0 = time.time()

        self.log(f"Starting stream: {stream_name}", "info")
        run = SyncRun(
            connector_name=connector_name,
            stream_name=stream_name,
            started_at=started_at,
            status="running",
        )
        self._state.add_sync_run(run)

        headers = self._build_headers()
        base_params = dict(stream.get("params") or {})

        # Resolve Calendly parent-stream dependencies before fetching
        base_url = self.manifest.get("base_url", "")
        if "calendly" in base_url.lower() and self.credential:
            base_params = self._inject_calendly_params(stream, base_params, headers)

        # Checkpoint check
        checkpoint = self._state.get_checkpoint(connector_name, stream_name)
        cursor = None
        if sync_mode == "incremental" and checkpoint:
            cursor = checkpoint.get("next_cursor")
            self.log(f"Resuming from checkpoint: cursor={str(cursor)[:40] if cursor else 'None'}", "info")

        total_records = 0
        pages = 0
        table = None

        # Emit auth stage
        self.progress(stage="auth", connector_name=connector_name, stream=stream_name,
                     message=f"Auth headers prepared — {self.manifest.get('auth', {}).get('type', 'none')}")

        # Emit partition stage
        self.progress(stage="partition", connector_name=connector_name, stream=stream_name,
                     message=f"Partition strategy: page-based")

        pagination = stream.get("pagination") or {}
        strategy = pagination.get("strategy", "none")
        page_size = pagination.get("page_size", 30)
        max_pages = 10  # Safety limit for demo

        # For link_header (GitHub), cursor is the full next URL
        is_link_header = strategy == "link_header"
        next_url = None

        for page_num in range(1, max_pages + 1):
            self.progress(
                stage="fetch",
                connector_name=connector_name,
                stream=stream_name,
                page=page_num,
                total_records=total_records,
                message=f"Fetching page {page_num}...",
            )

            if is_link_header and next_url and page_num > 1:
                # Fetch next URL directly
                try:
                    r = requests.get(next_url, headers=headers, timeout=10)
                    if r.status_code != 200:
                        break
                    records = _apply_selector(r.json(), stream.get("record_selector", "$[*]"))
                    link = r.headers.get("Link", "")
                    match = re.search(r'<([^>]+)>;\s*rel="next"', link)
                    next_url = match.group(1) if match else None
                except Exception as e:
                    self.log(f"Page fetch error: {e}", "error")
                    break
            else:
                records, next_cursor_val = self._fetch_page(stream, base_params, headers, cursor)

                if is_link_header:
                    # next_cursor_val is a full URL
                    next_url = next_cursor_val
                else:
                    cursor = next_cursor_val

            if not records:
                self.log(f"No records on page {page_num} — stream complete", "info")
                break

            pages += 1
            total_records += len(records)

            # Create table on first batch
            if table is None and records:
                table = self._create_table(stream_name, records[0], connector_name)
                self.log(f"Destination table: {table} ({len(records[0])} columns)", "info")

            # Write records
            self._insert_records(table or stream_name, records)
            self.log(f"Page {page_num}: {len(records)} records → {table}", "success")

            # Checkpoint
            checkpoint_data = {
                "stream": stream_name,
                "page": page_num,
                "next_cursor": next_url if is_link_header else cursor,
                "records_synced": total_records,
            }
            self._state.save_checkpoint(connector_name, stream_name, checkpoint_data)

            self.progress(
                stage="checkpoint",
                connector_name=connector_name,
                stream=stream_name,
                page=page_num,
                total_records=total_records,
                message=f"Checkpoint saved — {total_records} records",
                records=records[:3],
            )

            time.sleep(0.3)  # Pacing

            # Stop if no next page
            if strategy == "none":
                break
            if is_link_header and not next_url:
                break
            if not is_link_header and not cursor:
                break

        duration = time.time() - t0
        self.log(f"Stream complete: {total_records} records, {pages} pages, {duration:.1f}s", "success")

        self._state.update_sync_run(
            connector_name, started_at,
            {
                "status": "completed",
                "records_synced": total_records,
                "pages_fetched": pages,
                "completed_at": datetime.utcnow().isoformat(),
                "duration_seconds": duration,
            }
        )

        self.progress(
            stage="complete",
            connector_name=connector_name,
            stream=stream_name,
            total_records=total_records,
            pages=pages,
            duration=duration,
            message="Sync complete",
        )

        return SyncResult(
            connector_name=connector_name,
            stream_name=stream_name,
            records_synced=total_records,
            pages_fetched=pages,
            duration_seconds=duration,
            status="completed",
        )

    def run(self, connector_name: str, sync_mode: str = "full_refresh") -> list:
        """Run all streams for the connector. Returns list of SyncResult."""
        streams = self.manifest.get("streams", [])
        results = []
        for stream in streams:
            result = self.run_stream(stream, connector_name, sync_mode)
            results.append(result)
        return results

    def query_destination(self, connector_name: str, stream_name: str, limit: int = 50) -> list:
        """Query destination table and return records as list of dicts."""
        table = f"{connector_name}_{stream_name}".replace("-", "_").lower()
        try:
            conn = self._get_conn()
            cursor = conn.execute(f'SELECT * FROM "{table}" LIMIT {limit}')
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception:
            return []
