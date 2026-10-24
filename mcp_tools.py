"""StratCon's rubric actions as an MCP server. TrueForge agents call these through TrueFoundry."""
import json
import re

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

import brightdata
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


GRAPH_SCHEMA = """Nodes: Entity {id, kind: sponsor|talk|speaker|person, name, tier, description, org, when, room, url},
Topic {id, name}, Person {id, name, title, org}, Criterion {id, text}, User {id}, Evidence {url, product, snippet}.
Edges: (Person)-[:PRESENTS]->(Entity talk), (Entity talk)-[:PRESENTED_BY]->(Person), (Person)-[:WORKS_AT]->(Entity sponsor),
(Entity sponsor)-[:EMPLOYS]->(Person), (Entity)-[:ABOUT {p}]->(Topic), (Topic)-[:COVERS {p}]->(Entity),
(User)-[:WANTS {weight}]->(Criterion), (Criterion)-[:MAPS_TO {p}]->(Topic), (Entity)-[:SCORED {p}]->(Criterion),
(Evidence)-[:ABOUT]->(Entity), (Evidence)-[:INFORMS]->(Criterion)."""
WRITE = re.compile(r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|LOAD)\b", re.I)


def _plain(v):
    if hasattr(v, "properties"):
        return {k: x for k, x in v.properties.items() if k not in ("description",)} | {"_labels": getattr(v, "labels", None)}
    if isinstance(v, list):
        return [_plain(x) for x in v]
    return v


@mcp.tool()
async def current_recommendations(n: int = 5) -> str:
    """The attendee's pinned and top recommendations (id, name, kind, score, pinned). Use before answering questions about them."""
    s = engine.ACTIVE
    if s is None:
        return "No active StratCon session."
    ranked = s.ranking(everything=True)
    pinned = [r for r in ranked if r["id"] in s.pins]
    rows = (pinned + [r for r in ranked if r["score"] > 0 and r["id"] not in s.pins])[:max(len(pinned), min(n, 10))]
    return json.dumps([{"id": r["id"], "name": engine.ENTITIES[r["id"]]["name"], "kind": engine.ENTITIES[r["id"]]["kind"],
                        "score": r["score"], "pinned": r["id"] in s.pins} for r in rows])


@mcp.tool(description="Run a READ-ONLY Cypher query on the StratCon FalkorDB graph (FalkorDB Cypher; returns up to "
                       "20 rows). Use it to answer questions about sponsors, talks, people and how they connect. Schema: "
                       + GRAPH_SCHEMA)
async def graph_query(cypher: str) -> str:
    if WRITE.search(cypher):
        return json.dumps({"error": "read-only: CREATE/MERGE/SET/DELETE are not allowed"})
    q = cypher if re.search(r"\bLIMIT\b", cypher, re.I) else cypher.rstrip("; ") + " LIMIT 20"
    try:
        rows, ms = await engine.gq(q, {}, timeout=10)
    except Exception as x:
        return json.dumps({"error": str(x)[:300]})
    s = engine.ACTIVE
    if s is not None:
        await s.inbox.put(engine.ev("graph", op="agent_query", cypher=q, ms=ms, n=len(rows)))
    return json.dumps({"rows": [[_plain(v) for v in r] for r in rows[:20]], "ms": ms}, default=str)[:6000]


@mcp.tool()
async def research(entity: str, question: str) -> str:
    """Look something up on the live web with Bright Data (SERP API) about one sponsor, talk or person, e.g.
    research("JFrog", "open engineering positions"). Returns cited snippets and stores them as evidence in the graph."""
    s = engine.ACTIVE
    e = next((x for x in engine.ENTITIES.values() if x["id"] == entity or x["name"].lower() == entity.lower()), None)
    name = e["name"] if e else entity
    if s is not None:
        await s.inbox.put(engine.ev("fetch", e=e["id"] if e else "", c="", strategy="research", product="SERP API",
                                    target=f"{name} {question}"))
    r = await brightdata.serp(f"{name} {question}")
    hits = [{"title": x["title"], "snippet": x["description"], "url": x["link"]} for x in r["results"][:4]]
    if s is not None:
        await s.inbox.put(engine.ev("research", entity=name, question=question, ms=r["ms"], bytes=r["bytes"],
                                    hits=hits[:3]))
    if e and hits:
        try:
            await engine.gq("MATCH (e:Entity {id:$id}) CREATE (x:Evidence {url:$u, product:'SERP API', snippet:$s, "
                            "question:$q, at:timestamp()})-[:ABOUT]->(e)",
                            {"id": e["id"], "u": hits[0]["url"], "s": hits[0]["snippet"][:500], "q": question})
        except Exception:
            pass
    return json.dumps({"entity": name, "question": question, "results": hits, "product": "Bright Data SERP API",
                       "ms": r["ms"]})


class TokenGuard(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if config.MCP_TOKEN and request.headers.get("x-stratcon-token") != config.MCP_TOKEN:
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)


def app():
    a = mcp.streamable_http_app()
    a.add_middleware(TokenGuard)
    return a
