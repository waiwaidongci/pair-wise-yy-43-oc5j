from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib.parse import parse_qs, urlparse

from .domain import (ConflictError, DomainError, NotFoundError, PermissionDenied,
                     ValidationError)
from .service import Service


def make_handler(service: Service, static_dir: str):
    root = Path(static_dir)

    class Handler(BaseHTTPRequestHandler):
        server_version = "ModularHell/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _html(self, path: Path) -> None:
            if not path.exists():
                self._json(404, {"error": "not_found"})
                return
            body = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _identity(self) -> Tuple[str, str]:
            return self.headers.get("X-Actor", ""), self.headers.get("X-Role", "")

        def _body(self) -> Dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length <= 0:
                return {}
            if length > 8_000_000:
                raise ValidationError("请求体过大")
            try:
                value = json.loads(self.rfile.read(length).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError("请求体不是有效JSON") from exc
            if not isinstance(value, dict):
                raise ValidationError("请求体必须是JSON对象")
            return value

        def _send_error(self, exc: Exception) -> None:
            extra: Dict[str, Any] = {}
            if isinstance(exc, ValidationError):
                status = 422
            elif isinstance(exc, NotFoundError):
                status = 404
            elif isinstance(exc, PermissionDenied):
                status = 403
            elif isinstance(exc, ConflictError):
                status = 409
                match = re.search(r"#(\d+)", str(exc))
                if match:
                    extra["draft_id"] = int(match.group(1))
            elif isinstance(exc, ValueError):
                status = 422
            elif isinstance(exc, DomainError):
                status = 400
            else:
                status = 500
            payload = {"error": exc.__class__.__name__, "message": str(exc)}
            payload.update(extra)
            self._json(status, payload)

        def do_GET(self) -> None:
            try:
                path = urlparse(self.path).path
                query = parse_qs(urlparse(self.path).query)
                if path == "/health":
                    self._json(200, {"status": "ok"})
                elif path == "/":
                    self._html(root / "index.html")
                elif path == "/api/items":
                    _, role = self._identity()
                    status = query.get("status", [None])[0]
                    self._json(200, {"items": service.list_items(role, status)})
                elif path == "/api/batches":
                    _, role = self._identity()
                    batch_id = query.get("batch_id", [None])[0]
                    if not batch_id:
                        raise ValidationError("batch_id必填")
                    self._json(200, service.get_batch(batch_id, role))
                elif path == "/api/pending":
                    _, role = self._identity()
                    self._json(200, {"pending": service.list_pending(role)})
                elif path.startswith("/api/items/") and path.endswith("/records"):
                    item_id = int(path.split("/")[3])
                    _, role = self._identity()
                    self._json(200, {"records": service.list_records(item_id, role)})
                elif path.startswith("/api/items/") and path.endswith("/drafts"):
                    item_id = int(path.split("/")[3])
                    actor, role = self._identity()
                    self._json(200, {"drafts": service.list_drafts(item_id, actor, role)})
                elif path.startswith("/api/items/"):
                    item_id = int(path.rsplit("/", 1)[-1])
                    _, role = self._identity()
                    self._json(200, service.get_item(item_id, role))
                elif path == "/api/drafts":
                    actor, role = self._identity()
                    item_filter = query.get("item_id", [None])[0]
                    item_id = int(item_filter) if item_filter else None
                    self._json(200, {"drafts": service.list_drafts(item_id, actor, role)})
                elif path == "/api/audit":
                    _, role = self._identity()
                    self._json(200, {"events": service.audit(role)})
                else:
                    self._json(404, {"error": "not_found"})
            except Exception as exc:
                self._send_error(exc)

        def do_POST(self) -> None:
            try:
                path = urlparse(self.path).path
                actor, role = self._identity()
                body = self._body()
                if path == "/api/items":
                    self._json(201, service.create_item(body, actor, role))
                elif path == "/api/batches":
                    self._json(201, service.open_batch(body, actor, role))
                elif path.startswith("/api/batches/") and "/chunks/" in path:
                    parts = path.strip("/").split("/")
                    batch_id = parts[2]
                    chunk_index = int(parts[4])
                    self._json(201, service.upload_chunk(
                        batch_id, chunk_index, body, actor, role))
                elif path.startswith("/api/batches/") and path.endswith("/complete"):
                    batch_id = path.split("/")[3]
                    self._json(200, service.complete_batch(batch_id, actor, role))
                elif path.startswith("/api/pending/") and path.endswith("/adjudicate"):
                    pending_id = int(path.split("/")[3])
                    self._json(200, service.adjudicate(
                        pending_id, body, actor, role))
                elif path.startswith("/api/items/") and path.endswith("/records"):
                    item_id = int(path.split("/")[3])
                    self._json(201, service.add_record(item_id, body, actor, role))
                elif path.startswith("/api/items/") and path.endswith("/transition"):
                    item_id = int(path.split("/")[3])
                    self._json(200, service.transition(
                        item_id, body.get("target"),
                        body.get("expected_version"), actor, role))
                elif path.startswith("/api/items/") and path.endswith("/confirm-quantity"):
                    item_id = int(path.split("/")[3])
                    self._json(200, service.confirm_quantity(
                        item_id, body, actor, role))
                elif path.startswith("/api/items/") and path.endswith("/monitoring"):
                    item_id = int(path.split("/")[3])
                    self._json(200, service.monitoring_observation(
                        item_id, body, actor, role))
                elif path.startswith("/api/items/") and path.endswith("/rewind"):
                    item_id = int(path.split("/")[3])
                    self._json(200, service.rewind(item_id, body, actor, role))
                elif path.startswith("/api/drafts/") and path.endswith("/apply"):
                    draft_id = int(path.split("/")[3])
                    self._json(200, service.apply_draft(draft_id, body, actor, role))
                elif path.startswith("/api/drafts/") and path.endswith("/discard"):
                    draft_id = int(path.split("/")[3])
                    self._json(200, service.discard_draft(draft_id, actor, role))
                else:
                    self._json(404, {"error": "not_found"})
            except Exception as exc:
                self._send_error(exc)

    return Handler
