"""
CloudEagle — FastAPI backend.

Routes:
  GET  /                             → Marketplace SPA
  GET  /api/connectors               → List registered connectors + stats
  GET  /api/connectors/{name}        → Get single connector
  PUT  /api/connectors/{name}        → Update connector manifest
  POST /api/connectors/{name}/promote → Promote to production
  DELETE /api/connectors/{name}      → Delete connector
  POST /api/build/start              → Start AI build pipeline (returns build_id)
  GET  /api/build/{id}/events        → SSE stream for build progress
  POST /api/build/{id}/approve       → Approve build → save to registry
  POST /api/connectors/{name}/sync   → Start data sync (returns sync_id)
  GET  /api/sync/{id}/events         → SSE stream for sync progress
  POST /api/test                     → Quick connectivity test
  POST /api/chat                     → LLM-powered chat for AI Builder
  GET  /api/observability            → Sync history, checkpoints, stats
  GET  /api/destination/tables       → List SQLite destination tables
  GET  /api/destination/table/{name} → Query a destination table
  DELETE /api/observability/checkpoints → Clear all sync checkpoints
"""
import os
import sys
import json
import uuid
import queue
import threading
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

import requests as _requests
import anthropic as _anthropic

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).parent))
from core.registry import ConnectorRegistry
from core.state import StateManager
from core.ingestion import DocumentIngester
from core.ai_manifest_fill import (
    AIManifestFiller,
    CALENDLY_MANIFEST, CALENDLY_FILLED_FIELDS,
    GITHUB_MANIFEST,   GITHUB_FILLED_FIELDS,
    _mock_docs,
)
from core.validation import ValidationStack
from core.runtime import SyncRuntime


# ── Startup ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Pre-seed Calendly and GitHub connectors on first run."""
    seeds = [
        ("Calendly", CALENDLY_MANIFEST, CALENDLY_FILLED_FIELDS),
        ("GitHub",   GITHUB_MANIFEST,   GITHUB_FILLED_FIELDS),
    ]
    for cname, manifest, fields in seeds:
        if not reg.get_connector(cname):
            fields_out = [
                {"field_name": f.field_name, "value": f.value,
                 "citation_text": f.citation_text, "confidence": f.confidence}
                for f in fields
            ]
            acc = sum(1 for f in fields if f.confidence == "high") / max(len(fields), 1)
            reg.save_connector(
                name=cname, manifest=manifest, status="sandbox",
                field_accuracy=acc, fingerprint=f"seed_{cname.lower()}",
                filled_fields=fields_out,
                connector_docs=_mock_docs(cname),
            )
    yield


# ── App setup ─────────────────────────────────────────────────────────────────

reg   = ConnectorRegistry()
state = StateManager()

app = FastAPI(title="CloudEagle", lifespan=lifespan)
BASE_DIR  = Path(__file__).parent

# In-memory session store for active builds and syncs (keyed by UUID)
_sessions: dict[str, dict] = {}

# Connector catalogue — mirrors the frontend CONNECTORS constant
CONNECTORS = {
    "github":     {"display": "GitHub",     "icon": "🐙", "auth_label": "No auth needed", "url": "https://api.github.com",                  "needs_auth": False},
    "salesforce": {"display": "Salesforce", "icon": "☁️", "auth_label": "OAuth 2.0",      "url": "https://developer.salesforce.com",        "needs_auth": True},
    "hubspot":    {"display": "HubSpot",    "icon": "🟠", "auth_label": "API key",         "url": "https://developers.hubspot.com",          "needs_auth": True},
    "stripe":     {"display": "Stripe",     "icon": "💳", "auth_label": "API key",         "url": "https://stripe.com/docs/api",             "needs_auth": True},
}


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse((BASE_DIR / "templates" / "index.html").read_text())


# ── Connectors ────────────────────────────────────────────────────────────────

@app.get("/api/connectors")
def list_connectors():
    return {"connectors": reg.list_connectors(), "stats": reg.stats()}


@app.get("/api/connectors/{name}")
def get_connector(name: str):
    c = reg.get_connector(name)
    if not c:
        raise HTTPException(404, "Not found")
    return c


class SaveConnectorReq(BaseModel):
    manifest: dict
    status: str = "sandbox"
    field_accuracy: float = 0.95
    connector_docs: dict = None


@app.put("/api/connectors/{name}")
def save_connector(name: str, req: SaveConnectorReq):
    c = reg.get_connector(name)
    if not c:
        raise HTTPException(404)
    vi  = (c.get("versions") or [{}])[-1]
    docs = req.connector_docs if req.connector_docs is not None else vi.get("connector_docs")
    ver = reg.save_connector(
        name=name, manifest=req.manifest, status=req.status,
        field_accuracy=req.field_accuracy,
        fingerprint=vi.get("schema_fingerprint", ""),
        filled_fields=vi.get("filled_fields", []),
        connector_docs=docs,
    )
    return {"version": ver}


@app.post("/api/connectors/{name}/promote")
def promote(name: str):
    reg.promote_to_production(name)
    return {"ok": True}


@app.post("/api/connectors/{name}/demote")
def demote(name: str):
    reg.demote_to_sandbox(name)
    return {"ok": True}


@app.delete("/api/connectors/{name}")
def delete_connector(name: str):
    ok = reg.delete_connector(name)
    if not ok:
        raise HTTPException(404, "Not found")
    return {"ok": True}


# ── AI Build pipeline ─────────────────────────────────────────────────────────

class BuildReq(BaseModel):
    connector: str
    credential: str = ""
    claude_key: str = ""
    url: str = ""                   # docs URL for custom connectors not in the built-in registry
    base_url_override: str = ""     # override: actual API base URL (may differ from docs URL)
    auth_type: str = ""             # override from BUILD_READY (api_key|bearer|basic|none)
    auth_in: str = "header"         # override: where credential goes (header|query)
    auth_param_name: str = ""       # override: header/param name
    streams_override: list = []     # override: stream list with pre-filled params


@app.post("/api/build/start")
def start_build(req: BuildReq):
    """
    Launch the 3-step build pipeline in a background thread:
      Step 0 — Documentation ingestion
      Step 1 — AI manifest fill + docs generation
      Step 2 — 4-layer validation
    Progress events are streamed via SSE on /api/build/{id}/events.
    """
    cfg = next((v for v in CONNECTORS.values() if v["display"] == req.connector), None)
    if not cfg:
        # Allow custom connectors when a docs URL is supplied
        if req.url:
            cfg = {"display": req.connector, "url": req.url, "needsAuth": True}
        else:
            raise HTTPException(400, f"Unknown connector: {req.connector}. Provide a docs URL to build a custom connector.")

    build_id = str(uuid.uuid4())
    q: queue.Queue = queue.Queue()
    _sessions[build_id] = {"queue": q, "result": None}

    def run():
        try:
            def emit(event):
                q.put(event)

            def log_for(step):
                def cb(msg, lv="info"):
                    emit({"type": "log", "step": step, "level": lv, "msg": msg})
                return cb

            # Resolve Claude key: request field > environment variable > empty (mock mode)
            claude_key = req.claude_key or os.environ.get("ANTHROPIC_API_KEY", "")

            # ── Step 0: Documentation ingestion ──────────────────────────────
            emit({"type": "step_start", "step": 0})
            spec = DocumentIngester(log_callback=log_for(0)).ingest(cfg["url"], req.connector)
            emit({
                "type": "step_done", "step": 0,
                "summary": (
                    f"{spec.total_paths} endpoints · "
                    f"{spec.ingestion_strategy.replace('_', ' ').title()} · "
                    f"auth: {spec.auth_info.get('type', '?')}"
                ),
            })

            # ── Step 1: AI manifest fill ──────────────────────────────────────
            emit({"type": "step_start", "step": 1})

            field_rows = []
            def field_cb(field, value, citation, confidence):
                if field.startswith("__"):   # internal progress markers
                    return
                field_rows.append({
                    "field": field,
                    "value": str(value)[:45],
                    "citation": str(citation)[:60],
                    "confidence": confidence,
                })
                emit({"type": "field_row", "row": field_rows[-1]})

            filler  = AIManifestFiller(api_key=claude_key)
            manifest, filled = filler.fill(spec, req.connector, progress_callback=field_cb,
                                           streams_hint=req.streams_override or None)

            # ── Apply overrides collected by the chat assistant ───────────────
            # Base URL override — use exact API URL the user confirmed, not what the scraper guessed
            if req.base_url_override:
                manifest["base_url"] = req.base_url_override.rstrip("/")

            # Auth override (auth type, placement, param name)
            if req.auth_type:
                manifest.setdefault("auth", {})
                manifest["auth"]["type"] = req.auth_type
                manifest["auth"]["in"] = req.auth_in or "header"
                if req.auth_param_name:
                    manifest["auth"]["header_name"] = req.auth_param_name

            # Stream override — user explicitly chose which streams to include from the chat.
            # REPLACE the AI-generated stream list entirely; use AI data only to fill missing fields.
            if req.streams_override:
                ai_by_name = {s["name"]: s for s in manifest.get("streams", [])}
                # Also check fuzzy matches (e.g. "current weather" → "current_and_forecast")
                ai_by_name_lower = {k.replace("_", " ").lower(): v for k, v in ai_by_name.items()}
                new_streams = []
                for so in req.streams_override:
                    slug = so.get("name", "").replace("-", "_").replace(" ", "_").lower()
                    pretty = so.get("name", "")
                    # Find AI-generated stream to inherit path/selector/pagination
                    ai = ai_by_name.get(slug) or ai_by_name.get(pretty) or \
                         ai_by_name_lower.get(pretty.replace("_", " ").lower()) or {}
                    new_streams.append({
                        "name":            slug,
                        "path":            so.get("path") or ai.get("path", ""),
                        "record_selector": ai.get("record_selector", "$[*]"),
                        "primary_key":     ai.get("primary_key", "id"),
                        "pagination":      ai.get("pagination", {"strategy": "none"}),
                        "incremental":     ai.get("incremental", {"cursor_field": None}),
                        # Merge: AI params first, then user-provided params override
                        "params":          {**ai.get("params", {}), **so.get("params", {})},
                    })
                manifest["streams"] = new_streams

            high = sum(1 for f in filled if f.confidence == "high")
            emit({
                "type": "step_done", "step": 1,
                "summary": f"{len(filled)} fields filled · {high} HIGH confidence",
            })

            # Generate human-readable API docs (used in Docs tab)
            connector_docs = filler.generate_docs(spec, req.connector)
            emit({"type": "connector_docs", "docs": connector_docs})

            # ── Step 2: 4-layer validation ────────────────────────────────────
            emit({"type": "step_start", "step": 2})
            log2 = log_for(2)
            val  = ValidationStack(log_callback=log2)

            r1 = val.run_layer1_schema(manifest)
            log2(f"Schema validation: {'PASS' if r1.passed else 'FAIL'}", "success" if r1.passed else "error")

            r2 = val.run_layer2_secret_scan(manifest)
            log2(f"Secret scan: {'PASS' if r2.passed else 'FAIL'}", "success" if r2.passed else "error")

            r3 = val.run_layer3_probe(manifest, req.credential)
            log2(f"Live API probe: {'PASS' if r3.passed else 'FAIL'}", "success" if r3.passed else "error")
            for p in r3.detail.get("probes", []):
                log2(f"  {p['name']}: {p['detail']}", "success" if p.get("passed") else "warning")

            r4 = val.run_layer4_contract(manifest, req.credential)
            log2(f"Contract test: {'PASS' if r4.passed else 'FAIL'}", "success" if r4.passed else "error")

            all_ok = all(r.passed for r in [r1, r2, r3, r4])
            recs   = r4.detail.get("records_sample", [])
            emit({
                "type": "step_done", "step": 2,
                "summary": (
                    ("All 4 layers PASSED" if all_ok else "Some layers failed") +
                    (f" · {len(recs)} sample records" if recs else "")
                ),
            })

            # ── Review ready ──────────────────────────────────────────────────
            fields_out = [
                {"field_name": f.field_name, "value": f.value,
                 "citation_text": f.citation_text, "confidence": f.confidence}
                for f in filled
            ]
            _sessions[build_id]["result"] = {
                "manifest": manifest,
                "fields":   fields_out,
                "fingerprint":     r4.detail.get("schema_fingerprint", ""),
                "records_sample":  recs,
                "connector_docs":  connector_docs,
            }
            emit({"type": "review_ready", "fields": fields_out, "records_sample": recs})

        except Exception as e:
            q.put({"type": "error", "msg": str(e)})
        finally:
            q.put({"type": "_eof"})

    threading.Thread(target=run, daemon=True).start()
    return {"build_id": build_id}


@app.get("/api/build/{build_id}/events")
def build_events(build_id: str):
    """SSE endpoint — streams build pipeline progress to the browser."""
    session = _sessions.get(build_id)
    if not session:
        raise HTTPException(404)

    def generate():
        q = session["queue"]
        while True:
            try:
                ev = q.get(timeout=120)
                if ev.get("type") == "_eof":
                    break
                yield f"data: {json.dumps(ev)}\n\n"
                if ev.get("type") in ("review_ready", "error"):
                    break
            except queue.Empty:
                yield 'data: {"type":"ping"}\n\n'

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class ApproveReq(BaseModel):
    connector: str


@app.post("/api/build/{build_id}/approve")
def approve_build(build_id: str, req: ApproveReq):
    """Save approved build to the connector registry as a new version."""
    session = _sessions.get(build_id)
    if not session:
        raise HTTPException(400, "Build session not found — the server may have restarted. Please rebuild the connector.")
    if not session.get("result"):
        raise HTTPException(400, "Build not finished yet — wait for all pipeline steps to complete.")

    r   = session["result"]
    acc = sum(1 for f in r["fields"] if f.get("confidence") != "low") / max(len(r["fields"]), 1)
    ver = reg.save_connector(
        name=req.connector, manifest=r["manifest"], status="sandbox",
        field_accuracy=acc, fingerprint=r["fingerprint"],
        filled_fields=r["fields"], connector_docs=r.get("connector_docs"),
    )
    _sessions.pop(build_id, None)
    return {"version": ver}


# ── Data sync ─────────────────────────────────────────────────────────────────

class SyncReq(BaseModel):
    credential: str = ""
    mode: str = "full_refresh"


@app.get("/api/connectors/{name}/discover-streams")
def discover_streams(name: str):
    """Probe the connector's base_url for an OpenAPI spec and return discovered endpoints."""
    c = reg.get_connector(name)
    if not c:
        raise HTTPException(404)
    vi       = (c.get("versions") or [{}])[-1]
    manifest = vi.get("manifest_dict") or {}
    docs     = vi.get("connector_docs") or {}
    base_url = manifest.get("base_url", "")
    probe_url = docs.get("website") or base_url

    if not probe_url:
        return {"found": False, "endpoints": [], "strategy": None}

    import re as _re

    def _enrich_endpoints(raw_endpoints, strategy):
        endpoints = []
        for ep in raw_endpoints[:80]:
            name_hint = ""
            if ep.get("operation_id"):
                op = ep["operation_id"]
                op = _re.sub(r'(?<!^)(?=[A-Z])', '_', op).lower()
                op = _re.sub(r'^(list_|get_|fetch_|read_)', '', op)
                name_hint = op
            if not name_hint and ep.get("path"):
                segs = [s for s in ep["path"].split("/") if s and not s.startswith("{")]
                name_hint = segs[-1] if segs else ""
            if not name_hint and ep.get("tags"):
                name_hint = ep["tags"][0].lower().replace(" ", "_")
            name_hint = name_hint.replace("-", "_")[:40]

            schema_text = (ep.get("response_schema") or "").lower()
            selector = "$[*]"
            for key in ("items", "data", "results", "records", "list", "entries", "values"):
                if f"'{key}'" in schema_text or f'"{key}"' in schema_text:
                    selector = f"$.{key}"
                    break

            endpoints.append({
                **ep,
                "suggested_name":     name_hint,
                "suggested_selector": selector,
            })
        return endpoints

    try:
        ingester = DocumentIngester()
        spec = ingester._try_openapi(probe_url)
        if not spec and base_url and base_url != probe_url:
            spec = ingester._try_openapi(base_url)

        if spec:
            conn_spec = ingester._parse_openapi(spec, probe_url or base_url)
            endpoints = _enrich_endpoints(conn_spec.endpoints, conn_spec.ingestion_strategy)
            return {
                "found":     True,
                "strategy":  conn_spec.ingestion_strategy,
                "total":     conn_spec.total_paths,
                "endpoints": endpoints,
            }

        # No OpenAPI spec found — fall back to streams already known from connector_docs
        doc_streams = docs.get("streams") or []
        if doc_streams:
            fallback = []
            for s in doc_streams:
                path = s.get("path", "")
                segs = [seg for seg in path.split("/") if seg and not seg.startswith("{")]
                name_hint = s.get("name") or (segs[-1].replace("-", "_") if segs else "")
                fallback.append({
                    "path":               path,
                    "method":             "GET",
                    "summary":            s.get("description", ""),
                    "tags":               [],
                    "operation_id":       "",
                    "parameters":         [],
                    "response_schema":    "",
                    "suggested_name":     name_hint[:40],
                    "suggested_selector": "$[*]",
                })
            return {"found": True, "strategy": "connector_docs", "total": len(fallback), "endpoints": fallback}

        return {"found": False, "endpoints": [], "strategy": "none", "message": "No OpenAPI spec or known streams found."}
    except Exception as e:
        return {"found": False, "endpoints": [], "strategy": None, "message": str(e)}


@app.post("/api/connectors/{name}/sync")
def run_sync(name: str, req: SyncReq):
    """Start a full or incremental sync. Progress streamed via SSE."""
    c = reg.get_connector(name)
    if not c:
        raise HTTPException(404)

    vi       = (c.get("versions") or [{}])[-1]
    manifest = vi.get("manifest_dict") or {}
    sync_id  = str(uuid.uuid4())
    q: queue.Queue = queue.Queue()
    _sessions[sync_id] = {"queue": q}

    def run():
        def log_cb(msg, lv="info"):
            q.put({"type": "log", "level": lv, "msg": msg})
        try:
            rt      = SyncRuntime(manifest=manifest, credential=req.credential, log_callback=log_cb)
            results = rt.run(name, req.mode)
            total   = sum(r.records_synced for r in results)
            q.put({"type": "done", "total": total, "streams": len(results)})
            reg.increment_sync_count(name, records=total)
        except Exception as e:
            q.put({"type": "error", "msg": str(e)})
        finally:
            q.put({"type": "_eof"})

    threading.Thread(target=run, daemon=True).start()
    return {"sync_id": sync_id}


@app.get("/api/sync/{sync_id}/events")
def sync_events(sync_id: str):
    """SSE endpoint — streams sync progress to the browser."""
    session = _sessions.get(sync_id)
    if not session:
        raise HTTPException(404)

    def generate():
        q = session["queue"]
        try:
            while True:
                try:
                    ev = q.get(timeout=120)
                    if ev.get("type") == "_eof":
                        break
                    yield f"data: {json.dumps(ev)}\n\n"
                    if ev.get("type") in ("done", "error"):
                        break
                except queue.Empty:
                    yield 'data: {"type":"ping"}\n\n'
        finally:
            _sessions.pop(sync_id, None)  # clean up session once streaming is done

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Test connection ───────────────────────────────────────────────────────────

class ProbeReq(BaseModel):
    url: str
    method: str = "GET"
    headers: dict = {}


@app.post("/api/probe")
def probe_url(req: ProbeReq):
    """Probe a single URL and return its HTTP status."""
    import requests as _req
    try:
        r = _req.request(req.method, req.url, headers=req.headers, timeout=8, allow_redirects=True)
        return {"status": r.status_code, "ok": r.status_code < 500}
    except Exception as e:
        return {"status": 0, "ok": False, "error": str(e)}


class TestReq(BaseModel):
    manifest: dict
    credential: str = ""


@app.get("/api/data/{connector}/{stream}")
def preview_data(connector: str, stream: str, limit: int = 20):
    """Return the last N synced records for a connector stream from the destination DB."""
    rt = SyncRuntime(manifest={}, credential="")
    records = rt.query_destination(connector, stream, limit=limit)
    return {"records": records, "count": len(records)}


@app.post("/api/test")
def test_connection(req: TestReq):
    """Ping the base URL and up to 3 stream endpoints. Returns pass/fail per probe."""
    m        = req.manifest
    base_url = m.get("base_url", "").rstrip("/")
    auth     = m.get("auth") or {}
    atype    = auth.get("type", "none")
    auth_in  = auth.get("in", "header")   # "header" or "query"
    headers  = {"User-Agent": "CloudEagle/1.0", "Accept": "application/json"}
    # Query params injected for APIs that use key-as-query-param (e.g. ?appid=...)
    auth_params: dict = {}

    if req.credential:
        if atype == "bearer":
            headers["Authorization"] = f"Bearer {req.credential}"
        elif atype == "api_key":
            if auth_in == "query":
                auth_params[auth.get("header_name", "appid")] = req.credential
            else:
                headers[auth.get("header_name", "X-API-Key")] = req.credential

    results = []
    # Base URL connectivity — use first stream's params so required params (lat/lon etc.) are included.
    # Accept anything below 500 as "reachable" (400 = bad request from server = server is up).
    first_stream_params = {}
    if m.get("streams"):
        first_stream_params = {**m["streams"][0].get("params", {}), **auth_params}
    try:
        r = _requests.get(base_url, headers=headers, params=first_stream_params or auth_params, timeout=8)
        results.append({"name": "Base URL", "ok": r.status_code < 500,
                        "status": r.status_code, "url": r.url, "method": "GET", "note": ""})
    except Exception as e:
        results.append({"name": "Base URL", "ok": False,
                        "status": str(e), "url": base_url, "method": "GET", "note": ""})

    for s in (m.get("streams") or [])[:3]:
        endpoint_url = base_url
        method = (s.get("method") or "GET").upper()
        try:
            params = {**s.get("params", {}), **auth_params}
            path = s.get("path", "")
            if path and not path.startswith("/"):
                path = "/" + path
            endpoint_url = base_url + path
            if method == "POST":
                r = _requests.post(endpoint_url, headers=headers, params=auth_params,
                                   json=s.get("params", {}), timeout=8)
            else:
                r = _requests.get(endpoint_url, headers=headers, params=params, timeout=8)
            ok = r.status_code < 500
            note = ""
            if r.status_code == 400:
                note = "Missing required params — add them to the stream's Static params field (e.g. {\"lat\":\"51.51\",\"lon\":\"-0.13\"})"
            elif r.status_code == 401:
                note = "Auth failed — check your API key"
            elif r.status_code == 403:
                note = "Access denied — check permissions"
            elif r.status_code == 404:
                note = f"Path not found. Tested URL: {r.url} — verify the stream path is correct"
            elif r.status_code == 422:
                note = "Validation error — check required params"
            results.append({"name": s.get("name", ""), "ok": ok,
                            "status": r.status_code, "url": r.url,
                            "method": method, "note": note})
        except Exception as e:
            results.append({"name": s.get("name", ""), "ok": False,
                            "status": str(e), "url": endpoint_url,
                            "method": method, "note": ""})

    return {"results": results}


# ── LLM Chat (AI Builder) ─────────────────────────────────────────────────────

class ChatReq(BaseModel):
    message: str
    history: list = []
    connector: str = ""
    claude_key: str = ""


@app.post("/api/chat")
def chat(req: ChatReq):
    """
    LLM-powered chat for the AI Builder.
    Resolves the Claude key from: request field > ANTHROPIC_API_KEY env var.
    Falls back to keyword-based responses when no key is available.
    Returns {reply: str, action: dict|null}.

    When the LLM detects a credential in the conversation it embeds:
      CREDENTIAL_READY:{"credential":"...","connector":"..."}
    which is stripped from the reply and returned as action.type=credential_ready.
    """
    claude_key = req.claude_key or os.environ.get("ANTHROPIC_API_KEY", "")

    system_prompt = """You are an expert AI assistant inside CloudEagle that builds REST API connectors.

You have deep knowledge of popular APIs. Use that knowledge aggressively — only ask the user for things you genuinely cannot know yourself.

Things you MUST figure out yourself (never ask the user):
- The API base URL
- The auth type (api_key, bearer, oauth2, none)
- Where the API key goes (header vs query param) and what the param/header name is
- What endpoints exist and what paths they use
- What required parameters streams need (lat/lon, etc.) — use sensible defaults

Things you MUST ask the user (one at a time):
1. Which API they want to connect (if not clear from their message)
2. Their API key / token (you can never know this — always ask for it)
3. Which specific data streams they want to sync (this is a user preference, not something you can assume)

Flow:
- Once you know the API name, immediately state what you know about it (base URL, auth method, available streams) — do not ask the user to confirm things you already know
- Ask only for the credential, then which streams they want
- If the user doesn't know a detail (like lat/lon), use sensible defaults and tell them what you're using
- After the user specifies which streams they want, confirm once with a short summary and ask "Should we start building?"
- Only after they say yes, emit the marker on its own line:
BUILD_READY:{"connector":"<name>","url":"<docs_url>","base_url":"<base_url>","auth_type":"<api_key|bearer|basic|none>","auth_in":"<header|query>","auth_param_name":"<param name>","credential":"<exact credential>","streams":[{"name":"<stream>","path":"<path>","params":{<required params with defaults>}}]}

Rules:
- Keep each reply to 1–3 sentences
- Never ask more than one question per message
- Never ask about things you already know (base URL, auth type, param names)
- Never emit BUILD_READY before the user confirms
- No markdown headings, no --- dividers, no emojis unless user uses them"""

    messages = [{"role": m["role"], "content": m["content"]} for m in req.history]
    messages.append({"role": "user", "content": req.message})

    # ── Fallback (no API key) ──────────────────────────────────────────────
    if not claude_key:
        msg = req.message.lower()
        if any(w in msg for w in ["openweather", "weather", "forecast"]):
            reply = ("I can help you build a connector for OpenWeatherMap! It uses a simple API key for auth. "
                     "Could you share your OpenWeatherMap API key? You can get one free at openweathermap.org.")
        elif any(w in msg for w in ["api key", "token", "credential", "bearer"]):
            reply = "Thanks for providing that credential! Let me start building the connector now."
        else:
            reply = (
                "I'd love to help build a connector for that! To get started, could you tell me:\n"
                "1. The API documentation URL\n"
                "2. How it authenticates (API key, Bearer token, OAuth?)\n"
                "3. Which data you'd like to sync"
            )
        return {"reply": reply, "action": None}

    # ── Claude response ────────────────────────────────────────────────────
    try:
        client   = _anthropic.Anthropic(api_key=claude_key)
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            system=system_prompt,
            messages=messages,
        )
        raw_reply = response.content[0].text

        # Parse and strip BUILD_READY or CREDENTIAL_READY markers
        action = None
        clean_lines = []
        for line in raw_reply.split("\n"):
            stripped = line.strip()
            if stripped.startswith("BUILD_READY:"):
                try:
                    action = {"type": "build_ready", "data": json.loads(stripped[12:])}
                except Exception:
                    pass
            elif stripped.startswith("CREDENTIAL_READY:"):
                try:
                    action = {"type": "credential_ready", "data": json.loads(stripped[17:])}
                except Exception:
                    pass
            else:
                clean_lines.append(line)

        return {"reply": "\n".join(clean_lines).strip(), "action": action}

    except Exception as e:
        return {
            "reply": f"I'm ready to help you build a connector! (Note: {str(e)[:80]})",
            "action": None,
        }


# ── Observability ─────────────────────────────────────────────────────────────

@app.get("/api/observability")
def get_observability():
    state._state = state._load()          # always read fresh from disk
    history     = state.get_sync_history()
    checkpoints = state._state.get("checkpoints", {})
    completed   = sum(1 for h in history if h.get("status") == "completed")
    failed      = sum(1 for h in history if h.get("status") == "failed")
    total_rec   = sum(h.get("records_synced", 0) for h in history)
    avg_dur     = (
        sum(h.get("duration_seconds") or 0 for h in history if h.get("duration_seconds"))
        / max(completed, 1)
    )
    return {
        "history": history[:100],
        "checkpoints": [
            {
                "key": k,
                "connector": k.split(":")[0] if ":" in k else k,
                "stream":    k.split(":")[1] if ":" in k else "",
                **v,
            }
            for k, v in checkpoints.items()
        ],
        "stats": {
            "total_syncs":   len(history),
            "total_records": total_rec,
            "completed":     completed,
            "failed":        failed,
            "avg_duration":  round(avg_dur, 1),
        },
    }


# ── Destination data (SQLite) ─────────────────────────────────────────────────

DATA_DIR = BASE_DIR / "data"
DB_PATH  = DATA_DIR / "destination.db"


@app.get("/api/destination/tables")
def list_tables():
    if not DB_PATH.exists():
        return {"tables": []}
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        conn.close()
        return {"tables": [r[0] for r in rows]}
    except Exception as e:
        return {"tables": [], "error": str(e)}


@app.get("/api/destination/table/{table_name}")
def get_table_data(table_name: str, limit: int = 100):
    if not DB_PATH.exists():
        return {"columns": [], "rows": []}
    try:
        conn = sqlite3.connect(str(DB_PATH))
        # Validate table_name against the actual tables in the DB to prevent SQL injection
        known = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if table_name not in known:
            conn.close()
            raise HTTPException(404, f"Table '{table_name}' not found")
        cur  = conn.execute(f'SELECT * FROM "{table_name}" LIMIT ?', (limit,))
        cols = [d[0] for d in cur.description]
        rows = [list(r) for r in cur.fetchall()]
        conn.close()
        return {"columns": cols, "rows": rows}
    except HTTPException:
        raise
    except Exception as e:
        return {"columns": [], "rows": [], "error": str(e)}


@app.get("/api/debug/{connector_name}/{stream_name}")
def get_debug_request(connector_name: str, stream_name: str):
    state._state = state._load()          # always read fresh from disk
    data = state.get_last_request(connector_name, stream_name)
    if not data:
        return {"found": False}
    return {"found": True, **data}


@app.delete("/api/observability/checkpoints")
def clear_checkpoints():
    state._state["checkpoints"] = {}
    state._save()
    return {"ok": True}
