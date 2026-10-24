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


@mcp.tool()
async def remove_criterion(criterion_id: str) -> str:
    """Remove a criterion by id, when the attendee no longer cares about it."""
    return await _apply({"op": "remove", "id": criterion_id})


@mcp.tool()
async def reweight(criterion_id: str, weight: float) -> str:
    """Change how much a criterion matters (-1.0..1.0)."""
    return await _apply({"op": "reweight", "id": criterion_id, "weight": weight})


@mcp.tool()
async def set_scope(scope: str) -> str:
    """Restrict recommendations to 'all', 'sponsors' or 'speakers'."""
    return await _apply({"op": "scope", "value": scope})


class TokenGuard(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if config.MCP_TOKEN and request.headers.get("x-stratcon-token") != config.MCP_TOKEN:
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)


def app():
    a = mcp.streamable_http_app()
    a.add_middleware(TokenGuard)
    return a
