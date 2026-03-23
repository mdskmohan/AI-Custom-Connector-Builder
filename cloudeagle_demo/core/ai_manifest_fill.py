"""AI manifest fill using Claude API with grounded extraction."""
import json
import time
from dataclasses import dataclass
from typing import Optional, Callable

from .ingestion import ConnectorSpec

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


@dataclass
class FilledField:
    field_name: str
    value: object
    citation_text: str
    confidence: str  # high, medium, low
    source_chunk: Optional[str] = None


# ─── Pre-computed mock manifests ─────────────────────────────────────────────

CALENDLY_MANIFEST = {
    "connector_type": "rest",
    "connector_name": "Calendly",
    "version": "1.0.0",
    "auth": {"type": "bearer", "secret_ref": "CALENDLY_API_TOKEN"},
    "base_url": "https://api.calendly.com",
    "rate_limit": {"requests_per_minute": 100},
    "streams": [
        {
            "name": "users",
            "path": "/users/me",
            "record_selector": "$.resource",
            "primary_key": "uri",
            "pagination": {"strategy": "none"},
            "incremental": {"cursor_field": "updated_at"},
            "params": {},
        },
        {
            "name": "event_types",
            "path": "/event_types",
            "record_selector": "$.collection",
            "primary_key": "uri",
            "pagination": {
                "strategy": "cursor",
                "cursor_field": "$.pagination.next_page",
                "cursor_param": "page_token",
                "page_size": 100,
            },
            "incremental": {"cursor_field": "updated_at"},
            "params": {"count": 100},
        },
        {
            "name": "scheduled_events",
            "path": "/scheduled_events",
            "record_selector": "$.collection",
            "primary_key": "uri",
            "pagination": {
                "strategy": "cursor",
                "cursor_field": "$.pagination.next_page",
                "cursor_param": "page_token",
                "page_size": 100,
            },
            "incremental": {"cursor_field": "created_at"},
            "params": {"count": 100},
        },
    ],
    "error_handling": {
        "retry_strategy": "exponential_backoff",
        "retryable_codes": [429, 500, 502, 503],
        "max_retries": 3,
    },
}

GITHUB_MANIFEST = {
    "connector_type": "rest",
    "connector_name": "GitHub",
    "version": "1.0.0",
    "name": "GitHub",
    "auth": {"type": "none"},
    "base_url": "https://api.github.com",
    "rate_limit": {"requests_per_minute": 60},
    "streams": [
        {
            "name": "releases",
            "path": "/repos/microsoft/vscode/releases",
            "method": "GET",
            "record_selector": "$[*]",
            "primary_key": "id",
            "pagination": {"strategy": "link_header", "page_size": 30},
            "incremental": {"cursor_field": "published_at"},
            "params": {"per_page": 30},
        },
        {
            "name": "contributors",
            "path": "/repos/microsoft/vscode/contributors",
            "method": "GET",
            "record_selector": "$[*]",
            "primary_key": "id",
            "pagination": {"strategy": "link_header", "page_size": 30},
            "params": {"per_page": 30},
        },
    ],
    "error_handling": {
        "retry_strategy": "exponential_backoff",
        "retryable_codes": [429, 500, 502, 503],
        "max_retries": 3,
    },
}

GITHUB_FILLED_FIELDS = [
    FilledField("connector.type", "rest", "GitHub provides a REST API at api.github.com", "high"),
    FilledField("auth.type", "none", "Public endpoints require no auth; rate limited to 60 req/hour unauthenticated", "high"),
    FilledField("base_url", "https://api.github.com", "Base URL: https://api.github.com", "high"),
    FilledField("rate_limit.requests_per_minute", 60, "60 requests per hour unauthenticated", "high"),
    FilledField("streams[0].name", "releases", "GET /repos/{owner}/{repo}/releases — List releases", "high"),
    FilledField("streams[0].path", "/repos/microsoft/vscode/releases", "List releases for microsoft/vscode", "high"),
    FilledField("streams[0].record_selector", "$[*]", "Releases endpoint returns a JSON array directly", "high"),
    FilledField("streams[0].primary_key", "id", "Each release has a unique integer id", "high"),
    FilledField("streams[0].pagination.strategy", "link_header", "GitHub uses Link header with rel=next for pagination", "high"),
    FilledField("streams[1].name", "contributors", "GET /repos/{owner}/{repo}/contributors — List contributors", "high"),
    FilledField("streams[1].path", "/repos/microsoft/vscode/contributors", "List contributors to microsoft/vscode", "high"),
    FilledField("error_handling.retry_strategy", "exponential_backoff", "Retry on rate limit and server errors", "medium"),
]


CALENDLY_FILLED_FIELDS = [
    FilledField("connector.type", "rest", "Calendly is a REST API", "high"),
    FilledField("auth.type", "bearer", "All API requests require a Bearer token in the Authorization header", "high"),
    FilledField("auth.secret_ref", "CALENDLY_API_TOKEN", "Generate a Personal Access Token from your Calendly account settings", "high"),
    FilledField("base_url", "https://api.calendly.com", "Base URL for all API endpoints: https://api.calendly.com", "high"),
    FilledField("rate_limit.requests_per_minute", 100, "Rate limit: 100 requests per minute per access token", "high"),
    FilledField("streams[0].name", "users", "GET /users/me — Returns basic information about current user", "high"),
    FilledField("streams[0].path", "/users/me", "GET /users/me endpoint returns the current user resource", "high"),
    FilledField("streams[0].record_selector", "$.resource", "Response envelope: { resource: {...} } for single resources", "high"),
    FilledField("streams[0].primary_key", "uri", "Each resource has a unique URI identifier", "high"),
    FilledField("streams[0].pagination.strategy", "none", "/users/me returns a single resource, no pagination needed", "high"),
    FilledField("streams[1].name", "event_types", "GET /event_types — Returns all Event Types associated with a specified User", "high"),
    FilledField("streams[1].path", "/event_types", "GET /event_types returns paginated collection of event types", "high"),
    FilledField("streams[1].record_selector", "$.collection", "Collection responses use $.collection array selector", "high"),
    FilledField("streams[1].pagination.strategy", "cursor", "Pagination uses page_token cursor parameter", "high"),
    FilledField("streams[1].incremental.cursor_field", "updated_at", "updated_at field tracks last modification time", "high"),
    FilledField("error_handling.retry_strategy", "exponential_backoff", "Retry with exponential backoff on rate limit or server errors", "medium"),
    FilledField("error_handling.retryable_codes", [429, 500, 502, 503], "Retry on 429 Too Many Requests and 5xx server errors", "high"),
]



def _mock_docs(connector_name: str, spec=None) -> dict:
    """Return plausible mock documentation for a connector."""
    auth_type = (spec.auth_info.get("type", "api_key") if spec else "api_key")
    base_url  = spec.base_url if spec else ""
    streams   = []
    if spec and spec.endpoints:
        for ep in spec.endpoints[:6]:
            path = ep.get("path", "") if isinstance(ep, dict) else str(ep)
            name = path.strip("/").split("/")[0].replace("-", "_") or "data"
            streams.append({"name": name, "path": path, "description": f"Fetch {name} records"})

    how_to_get = {
        "bearer": [
            f"Sign in to your {connector_name} account",
            "Navigate to Settings → API or Developer section",
            "Generate a new API token and copy it",
        ],
        "api_key": [
            f"Sign in to your {connector_name} account",
            "Go to Settings → API Keys or Developer portal",
            "Create a new key and copy it",
        ],
        "none": ["No authentication required — this API is publicly accessible"],
        "oauth2": [
            f"Register an OAuth application in the {connector_name} developer portal",
            "Set your redirect URI and copy the Client ID / Secret",
            "Complete the OAuth authorization flow",
        ],
    }.get(auth_type, [f"Refer to the {connector_name} API documentation for authentication details"])

    return {
        "name": connector_name,
        "description": f"{connector_name} provides data access via a REST API. Use this connector to sync records into your destination.",
        "website": f"https://{connector_name.lower().replace(' ', '')}.com",
        "auth": {
            "type": auth_type,
            "description": f"Authentication via {auth_type.replace('_', ' ')}",
            "credential_label": "Bearer Token" if auth_type == "bearer" else "API Key" if auth_type == "api_key" else "Credential",
            "how_to_get": how_to_get,
        },
        "base_url": base_url,
        "streams": streams,
        "rate_limits": None,
        "notes": None,
    }


def _mock_fill_with_animation(
    connector_name: str,
    progress_callback: Optional[Callable] = None,
) -> tuple:
    """Return pre-computed manifest with simulated field-by-field filling."""
    name_lower = connector_name.lower()
    if "calendly" in name_lower:
        manifest = CALENDLY_MANIFEST
        fields = CALENDLY_FILLED_FIELDS
    else:
        manifest = GITHUB_MANIFEST
        fields = GITHUB_FILLED_FIELDS

    for f in fields:
        time.sleep(0.4)
        if progress_callback:
            progress_callback(f.field_name, f.value, f.citation_text, f.confidence)

    return manifest, fields


class AIManifestFiller:
    def __init__(self, api_key: str):
        self.api_key = api_key
        if ANTHROPIC_AVAILABLE and api_key:
            self.client = anthropic.Anthropic(api_key=api_key)
        else:
            self.client = None

    def generate_docs(self, spec: ConnectorSpec, connector_name: str) -> dict:
        """Generate human-readable connector documentation. Returns a docs dict."""
        if not self.client:
            return _mock_docs(connector_name, spec)
        try:
            endpoint_summary = "\n".join(
                f"  {ep.get('path','?')} [{ep.get('method','GET')}] — {ep.get('summary','')}"
                for ep in (spec.endpoints or [])[:20]
                if isinstance(ep, dict)
            ) or "  (endpoints not available)"

            prompt = f"""You are documenting the {connector_name} API connector for an engineer who needs to configure it.

API info:
- Base URL: {spec.base_url}
- Auth type: {spec.auth_info.get('type', 'unknown')}
- Total endpoints: {spec.total_paths}
- Ingestion strategy: {spec.ingestion_strategy}
- Sample endpoints:
{endpoint_summary}

Return ONLY a JSON object (no markdown) with this exact structure:
{{
  "name": "{connector_name}",
  "description": "2-3 sentences describing what this API does and what data is available",
  "website": "official API/developer docs URL",
  "auth": {{
    "type": "api_key|bearer|oauth2|basic|none",
    "description": "One sentence on how auth works for this API",
    "credential_label": "What to call the credential e.g. API Key, Bearer Token, Personal Access Token",
    "how_to_get": ["Step 1", "Step 2", "Step 3"]
  }},
  "base_url": "{spec.base_url}",
  "streams": [
    {{"name": "stream_name", "path": "/path", "description": "What data this returns"}}
  ],
  "rate_limits": "Describe rate limits or null if unknown",
  "notes": "Any important caveats or null"
}}"""

            message = self.client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = message.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            return json.loads(raw)
        except Exception:
            return _mock_docs(connector_name, spec)

    def fill(
        self,
        spec: ConnectorSpec,
        connector_name: str,
        progress_callback: Optional[Callable] = None,
        streams_hint: list = None,
    ) -> tuple:
        """Fill manifest fields with citations. Returns (manifest_dict, filled_fields)."""
        if not self.client or not self.api_key:
            return _mock_fill_with_animation(connector_name, progress_callback)

        try:
            return self._fill_with_claude(spec, connector_name, progress_callback, streams_hint)
        except Exception as e:
            # Fall back to mock on error
            if progress_callback:
                progress_callback("__error__", str(e), f"Claude API error: {e}", "low")
            return _mock_fill_with_animation(connector_name, progress_callback)

    def _fill_with_claude(
        self,
        spec: ConnectorSpec,
        connector_name: str,
        progress_callback: Optional[Callable] = None,
        streams_hint: list = None,
    ) -> tuple:
        prompt = self._build_prompt(spec, connector_name, streams_hint)

        if progress_callback:
            progress_callback("__start__", None, "Sending request to Claude claude-sonnet-4-6...", "high")

        message = self.client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = message.content[0].text.strip()
        # Strip markdown code blocks if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            # Try to extract JSON
            import re
            match = re.search(r"\{.*\}", raw, re.DOTALL)
            if match:
                result = json.loads(match.group(0))
            else:
                raise ValueError(f"Could not parse JSON from Claude response: {raw[:200]}")

        manifest, filled_fields = self._build_manifest(result, connector_name, progress_callback)
        return manifest, filled_fields

    def _build_prompt(self, spec: ConnectorSpec, connector_name: str, streams_hint: list = None) -> str:
        endpoints_sample = json.dumps(spec.endpoints[:8], indent=2)
        chunks_text = "\n".join(
            f"[{c['chunk_id']}] {c['text'][:300]}" for c in spec.raw_chunks[:5]
        )

        return f"""You are extracting connector configuration from API documentation for {connector_name}.

ConnectorSpec:
Title: {spec.title}
Base URL: {spec.base_url}
Auth detected: {json.dumps(spec.auth_info)}
Endpoints (first 8): {endpoints_sample}
Rate limit: {json.dumps(spec.rate_limit)}
Pagination: {json.dumps(spec.pagination_info)}

Documentation chunks (for citations):
{chunks_text}

STRICT RULES:
1. auth.type MUST be exactly one of: oauth2, api_key, bearer, basic, none
2. pagination.strategy MUST be exactly one of: cursor, offset, page_number, link_header, none
3. connector_type MUST be: rest
4. Use the ConnectorSpec data above as the primary source. If the spec is incomplete (e.g. HTML scraping returned few endpoints), use your training knowledge about this API to fill in accurate values — cite the API name + docs URL as source.
5. For streams, include any required query params in the params object (e.g. lat/lon for location APIs, org_id, date fields, etc.)
6. If a field is truly unknown and you have no knowledge of it: set value to null, confidence to "low"
{f'7. IMPORTANT: Generate ONLY these streams (user-selected): {json.dumps(streams_hint)}. Use the exact name and path provided. Fill in record_selector, primary_key, pagination, and params from the API docs.' if streams_hint else ''}

Return ONLY valid JSON — no markdown, no explanation:
{{
  "connector_type": "rest",
  "connector_name": "{connector_name}",
  "auth_type": "<oauth2|api_key|bearer|basic|none>",
  "auth_in": "<header|query|cookie> (where the credential is sent — most APIs use header)",
  "auth_header_name": "<e.g. X-API-Key, Authorization, appid — the param/header name>",
  "auth_secret_ref": "<env_var_name_or_null>",
  "auth_citation": "<quoted text>",
  "base_url": "<url>",
  "base_url_citation": "<quoted text>",
  "rate_limit_rpm": <integer or null>,
  "rate_limit_citation": "<quoted text or null>",
  "streams": [
    {{
      "name": "<stream_name>",
      "path": "<api_path>",
      "record_selector": "<jsonpath e.g. $[*] for array, $.data for nested array, $.current for single object>",
      "primary_key": "<field_name or null>",
      "pagination_strategy": "<cursor|offset|page_number|link_header|none>",
      "pagination_cursor_field": "<jsonpath or null>",
      "pagination_cursor_param": "<param_name or null>",
      "cursor_field": "<field_name or null>",
      "required_params": {{
        "<param_name>": "<example_value_or_placeholder e.g. lat=51.5, lon=-0.12, org_id=YOUR_ORG>"
      }},
      "citations": {{
        "path": "<cited text>",
        "record_selector": "<cited text>",
        "primary_key": "<cited text>"
      }}
    }}
  ],
  "retry_codes": [429, 500, 502, 503],
  "confidence_scores": {{
    "auth_type": "high",
    "base_url": "high",
    "rate_limit": "medium"
  }}
}}"""

    def _build_manifest(
        self,
        result: dict,
        connector_name: str,
        progress_callback: Optional[Callable] = None,
    ) -> tuple:
        filled_fields = []

        def emit(field_name, value, citation, confidence="high"):
            time.sleep(0.3)
            f = FilledField(field_name, value, citation or "Extracted from API spec", confidence)
            filled_fields.append(f)
            if progress_callback:
                progress_callback(field_name, value, f.citation_text, confidence)

        scores = result.get("confidence_scores", {})

        emit("connector.type", "rest", "REST API connector", "high")
        emit("auth.type", result.get("auth_type", "bearer"),
             result.get("auth_citation", "Auth type from spec"),
             scores.get("auth_type", "high"))
        emit("auth.secret_ref", result.get("auth_secret_ref"),
             "Secret reference from environment configuration", "high")
        emit("base_url", result.get("base_url", ""),
             result.get("base_url_citation", "Base URL from spec"),
             scores.get("base_url", "high"))
        emit("rate_limit.requests_per_minute", result.get("rate_limit_rpm"),
             result.get("rate_limit_citation") or "Rate limit from spec",
             scores.get("rate_limit", "medium"))

        streams = result.get("streams", [])
        for i, stream in enumerate(streams[:3]):
            cit = stream.get("citations", {})
            emit(f"streams[{i}].name", stream.get("name"), f"Stream {i} from spec", "high")
            emit(f"streams[{i}].path", stream.get("path"), cit.get("path", "Path from spec"), "high")
            emit(f"streams[{i}].record_selector", stream.get("record_selector"),
                 cit.get("record_selector", "Record selector from spec"), "medium")
            emit(f"streams[{i}].primary_key", stream.get("primary_key"),
                 cit.get("primary_key", "Primary key from spec"), "high")
            emit(f"streams[{i}].pagination.strategy", stream.get("pagination_strategy", "none"),
                 "Pagination strategy detected from spec", "medium")
            if stream.get("cursor_field"):
                emit(f"streams[{i}].incremental.cursor_field", stream.get("cursor_field"),
                     "Cursor field for incremental sync", "medium")

        emit("error_handling.retry_strategy", "exponential_backoff",
             "Retry with exponential backoff on transient errors", "high")
        emit("error_handling.retryable_codes", result.get("retry_codes", [429, 500, 502, 503]),
             "Retry on rate limit (429) and server errors (5xx)", "high")
        # Build manifest dict
        manifest = {
            "connector_type": "rest",
            "connector_name": connector_name,
            "version": "1.0.0",
            "auth": {
                "type": result.get("auth_type", "bearer"),
                "in": result.get("auth_in", "header"),
                "header_name": result.get("auth_header_name", "Authorization"),
                "secret_ref": result.get("auth_secret_ref"),
            },
            "base_url": result.get("base_url", ""),
            "rate_limit": {"requests_per_minute": result.get("rate_limit_rpm")} if result.get("rate_limit_rpm") else None,
            "streams": [
                {
                    "name": s.get("name", f"stream_{i}"),
                    "path": s.get("path", ""),
                    "record_selector": s.get("record_selector", "$[*]"),
                    "primary_key": s.get("primary_key", "id"),
                    "pagination": {
                        "strategy": s.get("pagination_strategy", "none"),
                        "cursor_field": s.get("pagination_cursor_field"),
                        "cursor_param": s.get("pagination_cursor_param"),
                    },
                    "incremental": {"cursor_field": s.get("cursor_field")},
                    # Include required params so probes and syncs don't fail with HTTP 400
                    "params": s.get("required_params") or {},
                }
                for i, s in enumerate(streams[:3])
            ],
            "error_handling": {
                "retry_strategy": "exponential_backoff",
                "retryable_codes": result.get("retry_codes", [429, 500, 502, 503]),
                "max_retries": 3,
            },
        }

        return manifest, filled_fields
