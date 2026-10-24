"""StratCon's rubric actions as an MCP server. TrueForge agents call these through TrueFoundry."""
import json

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

import config
import engine

mcp = FastMCP("stratcon", instructions="Edit the attendee's recommendation rubric for GitHub Universe 2026.",
              streamable_http_path="/mcp",
              transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))


async def _apply(op: dict) -> str:
    s = engine.ACTIVE
    if s is None:
        return "No active StratCon session."
    events = await engine.apply_ops(s, [op])
    s.ops.append(op)
    for e in events:
        await s.inbox.put(e)
    await s.inbox.put(engine.ev("rubric", criteria=list(s.criteria.values()), scope=s.scope))
    return json.dumps({"rubric": [{"id": c["id"], "text": c["text"], "weight": c["weight"]} for c in s.criteria.values()],
                       "scope": s.scope})


@mcp.tool()
async def get_rubric() -> str:
    """Return the attendee's current rubric: criteria ids, texts and weights, plus scope."""
    s = engine.ACTIVE
    if s is None:
        return "No active StratCon session."
    return json.dumps({"rubric": [{"id": c["id"], "text": c["text"], "weight": c["weight"]} for c in s.criteria.values()],
                       "scope": s.scope})


@mcp.tool()
async def add_criterion(text: str, weight: float) -> str:
    """Add a yes/no-checkable criterion (8 words or fewer). Weight 0.1..1.0, or negative (-1.0..-0.1) to avoid."""
    return await _apply({"op": "add", "text": text, "weight": weight})


def _unknown(ref: str) -> str:
    s = engine.ACTIVE
    ids = [{"id": c["id"], "text": c["text"]} for c in s.criteria.values()] if s else []
    return json.dumps({"error": f"no criterion matches {ref!r}; pass one of these ids", "criteria": ids})


@mcp.tool()
async def remove_criterion(criterion_id: str) -> str:
    """Remove a criterion (by id or exact text) ONLY when the attendee says it was a mistake or drops it."""
    if not engine.ACTIVE or not engine.resolve_crit(engine.ACTIVE, criterion_id):
        return _unknown(criterion_id)
    return await _apply({"op": "remove", "id": criterion_id})


@mcp.tool()
async def reweight(criterion_id: str, weight: float) -> str:
    """Change how much a criterion (by id or exact text) matters (-1.0..1.0)."""
    if not engine.ACTIVE or not engine.resolve_crit(engine.ACTIVE, criterion_id):
        return _unknown(criterion_id)
    return await _apply({"op": "reweight", "id": criterion_id, "weight": weight})


@mcp.tool()
async def set_scope(scope: str) -> str:
    """Restrict recommendations to 'all', 'sponsors', 'speakers' or 'talks'."""
    return await _apply({"op": "scope", "value": scope})


@mcp.tool()
async def set_clock(iso_datetime: str) -> str:
    """Set the current time at the conference (e.g. 2026-10-28T14:00) so past talks drop out and the plan updates."""
    return await _apply({"op": "clock", "value": iso_datetime})


@mcp.tool()
async def export_calendar() -> str:
    """Build the attendee's plan (best non-overlapping talks plus an expo-hall block) as a downloadable .ics calendar."""
    out = await _apply({"op": "calendar"})
    s = engine.ACTIVE
    url = (config.PUBLIC_URL.rstrip("/") if config.PUBLIC_URL else "") + f"/api/calendar.ics?sid={s.sid if s else ''}"
    return json.dumps({"calendar_url": url, "result": json.loads(out) if out.startswith("{") else out})


class TokenGuard(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if config.MCP_TOKEN and request.headers.get("x-stratcon-token") != config.MCP_TOKEN:
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)


def app():
    a = mcp.streamable_http_app()
    a.add_middleware(TokenGuard)
    return a
