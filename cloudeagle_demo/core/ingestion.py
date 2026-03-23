"""Doc ingestion: OpenAPI parser + HTML scraper."""
import re
import time
import requests
from dataclasses import dataclass, field
from typing import Optional, Callable
from urllib.parse import urljoin, urlparse

GITHUB_OPENAPI_URL = (
    "https://raw.githubusercontent.com/github/rest-api-description/main"
    "/descriptions/api.github.com/api.github.com.json"
)

OPENAPI_PROBE_PATHS = [
    "/openapi.json", "/swagger.json", "/api-docs",
    "/docs/openapi.json", "/v3/api-docs", "/openapi.yaml",
    "/swagger/v1/swagger.json",
]


@dataclass
class ConnectorSpec:
    title: str
    base_url: str
    auth_info: dict
    endpoints: list
    rate_limit: Optional[dict]
    pagination_info: Optional[dict]
    raw_chunks: list
    ingestion_strategy: str  # "openapi" or "html_scraper"
    completeness: dict
    openapi_version: Optional[str] = None
    total_paths: int = 0
    total_schemas: int = 0


class DocumentIngester:
    """
    Fetches and parses API documentation into a structured ConnectorSpec.

    Strategy (in order):
      1. Special-case GitHub — fetch from the canonical raw OpenAPI description repo.
      2. Probe common OpenAPI/Swagger paths on the API's root domain.
      3. Fall back to HTML scraping if no machine-readable spec is found.
    """

    def __init__(self, log_callback: Optional[Callable] = None):
        self.log = log_callback or (lambda msg, level="info": None)

    def ingest(self, url: str, connector_name: str) -> ConnectorSpec:
        """Entry point — try OpenAPI first, fall back to HTML scraper."""
        self.log(f"Starting ingestion for {connector_name}", "info")
        self.log(f"Source URL: {url}", "info")

        # Try OpenAPI first
        spec = self._try_openapi(url)
        if spec:
            self.log("OpenAPI spec detected — using structured parser", "success")
            return self._parse_openapi(spec, url)

        # Fall back to HTML scraper
        self.log("No OpenAPI spec found — falling back to HTML scraper", "warning")
        return self._scrape_html(url)

    def _try_openapi(self, base_url: str) -> Optional[dict]:
        """Probe known OpenAPI/Swagger paths. Returns parsed JSON or None."""
        # Special case for GitHub
        if "github.com" in base_url or "api.github.com" in base_url:
            self.log(f"GitHub detected — fetching from canonical OpenAPI location", "info")
            try:
                r = requests.get(GITHUB_OPENAPI_URL, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    if "openapi" in data or "swagger" in data:
                        self.log(f"GitHub OpenAPI spec fetched ({len(r.content)//1024} KB)", "success")
                        return data
            except Exception as e:
                self.log(f"GitHub spec fetch failed: {e}", "warning")

        # Probe common paths
        base = base_url.rstrip("/")
        parsed = urlparse(base)
        probe_base = f"{parsed.scheme}://{parsed.netloc}"

        for path in OPENAPI_PROBE_PATHS:
            probe_url = probe_base + path
            self.log(f"Checking for OpenAPI spec at {path}...", "info")
            try:
                r = requests.get(probe_url, timeout=5)
                if r.status_code == 200:
                    content_type = r.headers.get("content-type", "")
                    try:
                        data = r.json()
                        if "openapi" in data or "swagger" in data or "paths" in data:
                            self.log(f"OpenAPI spec found at {probe_url}", "success")
                            return data
                    except Exception:
                        pass
            except Exception:
                continue

        return None

    def _parse_openapi(self, spec: dict, source_url: str) -> ConnectorSpec:
        """Parse an OpenAPI 3.x or Swagger 2.x spec dict into a ConnectorSpec."""
        info = spec.get("info", {})
        title = info.get("title", "Unknown API")
        version = spec.get("openapi") or spec.get("swagger", "unknown")

        self.log(f"Parsing OpenAPI spec: {title} (OpenAPI {version})", "info")

        # Base URL
        base_url = ""
        servers = spec.get("servers", [])
        if servers:
            base_url = servers[0].get("url", "").rstrip("/")
        if not base_url:
            # Try from source
            parsed = urlparse(source_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"

        self.log(f"Base URL: {base_url}", "info")

        # Parse paths
        paths = spec.get("paths", {})
        total_paths = len(paths)
        self.log(f"Found {total_paths} API paths", "info")

        endpoints = []
        for path, methods in list(paths.items())[:50]:  # Cap for memory
            for method, op in methods.items():
                if method not in ("get", "post", "put", "patch", "delete"):
                    continue
                endpoint = {
                    "path": path,
                    "method": method.upper(),
                    "summary": op.get("summary", ""),
                    "description": (op.get("description") or "")[:200],
                    "parameters": [
                        {
                            "name": p.get("name"),
                            "in": p.get("in"),
                            "required": p.get("required", False),
                            "description": (p.get("description") or "")[:100],
                        }
                        for p in op.get("parameters", [])[:10]
                    ],
                    "tags": op.get("tags", []),
                    "operation_id": op.get("operationId", ""),
                }
                # Response schema
                responses = op.get("responses", {})
                if "200" in responses:
                    content = responses["200"].get("content", {})
                    json_content = content.get("application/json", {})
                    endpoint["response_schema"] = str(json_content.get("schema", {}))[:300]
                endpoints.append(endpoint)

        self.log(f"Parsed {len(endpoints)} endpoints (capped at 50 paths)", "info")

        # Auth
        auth_info = self._extract_auth(spec)
        self.log(f"Auth type detected: {auth_info.get('type', 'unknown')}", "info")

        # Rate limit (from extensions)
        rate_limit = self._extract_rate_limit(spec)
        if rate_limit:
            self.log(f"Rate limit found: {rate_limit.get('requests_per_minute')} req/min", "success")
        else:
            self.log("Rate limit: not found in spec extensions", "warning")

        # Pagination
        pagination_info = self._detect_pagination(endpoints)
        if pagination_info:
            self.log(f"Pagination pattern detected: {pagination_info.get('strategy')}", "success")

        # Schemas
        components = spec.get("components", spec.get("definitions", {}))
        schemas = components.get("schemas", {}) if isinstance(components, dict) else {}
        total_schemas = len(schemas)
        self.log(f"Schemas defined: {total_schemas}", "info")

        # Completeness check
        completeness = {
            "auth": bool(auth_info.get("type") and auth_info["type"] != "unknown"),
            "endpoints": len(endpoints) > 0,
            "rate_limit": bool(rate_limit),
            "pagination": bool(pagination_info),
        }
        missing = [k for k, v in completeness.items() if not v]
        if missing:
            self.log(f"Completeness check — missing: {', '.join(missing)}", "warning")
        else:
            self.log("Completeness check: all sections found", "success")

        # Raw chunks for citations
        raw_chunks = self._build_chunks_from_openapi(spec, source_url)

        return ConnectorSpec(
            title=title,
            base_url=base_url,
            auth_info=auth_info,
            endpoints=endpoints,
            rate_limit=rate_limit,
            pagination_info=pagination_info,
            raw_chunks=raw_chunks,
            ingestion_strategy="openapi",
            completeness=completeness,
            openapi_version=version,
            total_paths=total_paths,
            total_schemas=total_schemas,
        )

    def _extract_auth(self, spec: dict) -> dict:
        """Infer auth type from securitySchemes or info description."""
        components = spec.get("components", spec.get("securityDefinitions", {}))
        if isinstance(components, dict):
            security_schemes = components.get("securitySchemes", components)
        else:
            security_schemes = {}

        for name, scheme in security_schemes.items():
            scheme_type = scheme.get("type", "").lower()
            if scheme_type == "oauth2":
                flows = scheme.get("flows", {})
                token_url = ""
                for flow_name, flow in flows.items():
                    token_url = flow.get("tokenUrl", "")
                return {"type": "oauth2", "token_url": token_url, "scheme_name": name}
            elif scheme_type == "apikey":
                return {
                    "type": "api_key",
                    "in": scheme.get("in", "header"),
                    "header_name": scheme.get("name", "X-API-Key"),
                    "scheme_name": name,
                }
            elif scheme_type == "http":
                http_scheme = scheme.get("scheme", "").lower()
                if http_scheme == "bearer":
                    return {"type": "bearer", "scheme_name": name}
                elif http_scheme == "basic":
                    return {"type": "basic", "scheme_name": name}

        # Detect from info
        description = spec.get("info", {}).get("description", "").lower()
        if "bearer" in description or "personal access token" in description:
            return {"type": "bearer"}
        if "api key" in description or "api_key" in description:
            return {"type": "api_key", "header_name": "Authorization"}

        return {"type": "unknown"}

    def _extract_rate_limit(self, spec: dict) -> Optional[dict]:
        """Extract rate limit from x-ratelimit extensions or info description text."""
        # Check x-ratelimit extensions
        info = spec.get("info", {})
        for key in ["x-ratelimit", "x-rate-limit", "x-throttle"]:
            if key in info:
                return {"requests_per_minute": info[key].get("limit", 60)}

        # Check description for rate limit mentions
        desc  = (info.get("description") or "").lower()
        match = re.search(r"(\d+)\s*requests?\s*(?:per|/)\s*minute", desc)
        if match:
            return {"requests_per_minute": int(match.group(1))}

        return None

    def _detect_pagination(self, endpoints: list) -> Optional[dict]:
        """Heuristically detect the pagination strategy from endpoint parameter names."""
        # Check for common pagination patterns in parameters
        cursor_params = {"page_token", "cursor", "next_page", "after", "before"}
        offset_params = {"offset", "skip"}
        page_params = {"page", "page_number"}

        for endpoint in endpoints:
            param_names = {p["name"] for p in endpoint.get("parameters", []) if p.get("name")}
            if param_names & cursor_params:
                return {"strategy": "cursor", "cursor_param": list(param_names & cursor_params)[0]}
            if param_names & offset_params:
                return {"strategy": "offset"}
            if param_names & page_params:
                return {"strategy": "page_number"}

        # Check response schemas for Link header hints
        return {"strategy": "link_header"}

    def _build_chunks_from_openapi(self, spec: dict, source_url: str) -> list:
        chunks = []
        info = spec.get("info", {})

        # Info chunk
        if info.get("description"):
            chunks.append({
                "chunk_id": "info.description",
                "section_path": "info.description",
                "text": info["description"][:500],
                "url": source_url,
            })

        # Auth chunk
        components = spec.get("components", {})
        if isinstance(components, dict):
            schemes = components.get("securitySchemes", {})
            for name, scheme in schemes.items():
                chunks.append({
                    "chunk_id": f"security.{name}",
                    "section_path": f"components.securitySchemes.{name}",
                    "text": str(scheme)[:300],
                    "url": source_url,
                })

        # Endpoint chunks (first 10)
        paths = spec.get("paths", {})
        for i, (path, methods) in enumerate(list(paths.items())[:10]):
            for method, op in methods.items():
                if method not in ("get", "post", "put", "patch", "delete"):
                    continue
                text = f"{method.upper()} {path}: {op.get('summary', '')} — {op.get('description', '')[:200]}"
                chunks.append({
                    "chunk_id": f"path.{i}.{method}",
                    "section_path": f"paths.{path}.{method}",
                    "text": text,
                    "url": source_url,
                })

        return chunks

    def _scrape_html(self, url: str) -> ConnectorSpec:
        self.log(f"HTML scraping: {url}", "info")
        try:
            headers = {"User-Agent": "CloudEagle-Ingestion/1.0"}
            r = requests.get(url, timeout=10, headers=headers)
            self.log(f"Fetched {url} — {r.status_code} ({len(r.content)//1024} KB)", "info")

            text = r.text
            # Extract title
            title_match = re.search(r"<title[^>]*>(.*?)</title>", text, re.IGNORECASE)
            title = title_match.group(1).strip() if title_match else "Unknown API"
            title = re.sub(r"<[^>]+>", "", title)

            # Extract text content
            clean = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
            clean = re.sub(r"<style[^>]*>.*?</style>", "", clean, flags=re.DOTALL)
            clean = re.sub(r"<[^>]+>", " ", clean)
            clean = re.sub(r"\s+", " ", clean).strip()

            # Detect auth
            auth_info = {"type": "unknown"}
            text_lower = clean.lower()
            if "bearer" in text_lower or "personal access token" in text_lower:
                auth_info = {"type": "bearer"}
            elif "api key" in text_lower or "x-api-key" in text_lower:
                # Check if docs mention query-param style (e.g. ?apikey=, ?appid=, ?key=)
                in_query = bool(re.search(r"\?(?:api_?key|appid|key|token)=", text_lower))
                auth_info = {"type": "api_key", "in": "query" if in_query else "header",
                             "header_name": "appid" if in_query else "X-API-Key"}
            elif "oauth" in text_lower:
                auth_info = {"type": "oauth2"}

            # Extract base URL — try api.* subdomains first, then fall back to any explicit URL
            base_url_match = (
                re.search(r"https?://api\.[a-z0-9.-]+(?:/[a-z0-9._/-]+)?", clean) or
                re.search(r"https?://[a-z0-9.-]+/(?:api|v\d|data)/[a-z0-9._/-]*", clean)
            )
            base_url = base_url_match.group(0).rstrip("/") if base_url_match else url

            # Rate limit
            rate_match = re.search(r"(\d+)\s*requests?\s*(?:per|/)\s*minute", clean, re.IGNORECASE)
            rate_limit = {"requests_per_minute": int(rate_match.group(1))} if rate_match else None

            # Endpoints (find GET/POST patterns)
            endpoints = []
            endpoint_matches = re.finditer(r"(GET|POST|PUT|DELETE|PATCH)\s+(/[a-zA-Z0-9/_{}.-]+)", clean)
            for m in list(endpoint_matches)[:30]:
                endpoints.append({
                    "method": m.group(1),
                    "path": m.group(2),
                    "summary": "",
                    "parameters": [],
                })
            self.log(f"Scraped {len(endpoints)} endpoints from HTML", "info")

            completeness = {
                "auth": auth_info["type"] != "unknown",
                "endpoints": len(endpoints) > 0,
                "rate_limit": bool(rate_limit),
                "pagination": "cursor" in text_lower or "page" in text_lower,
            }
            missing = [k for k, v in completeness.items() if not v]
            if missing:
                self.log(f"Completeness: missing {', '.join(missing)}", "warning")
            else:
                self.log("Completeness check: all sections found", "success")

            chunks = [{"chunk_id": "html.body", "section_path": "body", "text": clean[:2000], "url": url}]

            return ConnectorSpec(
                title=title,
                base_url=base_url,
                auth_info=auth_info,
                endpoints=endpoints,
                rate_limit=rate_limit,
                pagination_info={"strategy": "cursor"} if completeness["pagination"] else None,
                raw_chunks=chunks,
                ingestion_strategy="html_scraper",
                completeness=completeness,
                total_paths=len(endpoints),
                total_schemas=0,
            )
        except Exception as e:
            self.log(f"HTML scraping error: {e}", "error")
            # Return minimal spec
            return ConnectorSpec(
                title="Unknown API",
                base_url=url,
                auth_info={"type": "bearer"},
                endpoints=[],
                rate_limit=None,
                pagination_info=None,
                raw_chunks=[],
                ingestion_strategy="html_scraper",
                completeness={"auth": False, "endpoints": False, "rate_limit": False, "pagination": False},
            )
