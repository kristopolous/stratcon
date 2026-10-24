import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

import engine
import mcp_tools

HERE = os.path.dirname(__file__)
sessions: dict[str, engine.Session] = {}
mcp_app = mcp_tools.app()


@asynccontextmanager
async def lifespan(_):
    await engine.load_entities()
    asyncio.create_task(engine.warmup())
    async with mcp_tools.mcp.session_manager.run():
        yield


app = FastAPI(title="StratCon", lifespan=lifespan)
app.mount("/tools", mcp_app)


def session(sid: str) -> engine.Session:
    if sid not in sessions:
        sessions[sid] = engine.Session(sid)
    return sessions[sid]


@app.get("/")
async def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"), headers={"Cache-Control": "no-store"})


@app.get("/api/entities")
async def entities():
    return JSONResponse(list(engine.ENTITIES.values()))


@app.post("/api/turn")
async def turn(req: Request):
    body = await req.json()
    s = session(body["sid"])

    async def stream():
        try:
            async for e in engine.turn(s, body["text"]):
                yield f"data: {json.dumps(e)}\n\n"
        except Exception as x:
            yield f"data: {json.dumps(engine.ev('error', text=str(x)[:300]))}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/graph")
async def graph(sid: str):
    s = session(sid)
    top = [r["id"] for r in s.ranking()[:10] if r["score"] > 0]
    cy = ("MATCH (u:User {id:$sid})-[w:WANTS]->(c:Criterion) "
          "OPTIONAL MATCH (e:Entity)-[sc:SCORED]->(c) WHERE e.id IN $top AND sc.p > 0.5 "
          "OPTIONAL MATCH (x:Evidence)-[:ABOUT]->(e) WHERE (x)-[:INFORMS]->(c) "
          "RETURN c.id, c.text, w.weight, e.id, e.name, e.kind, sc.p, id(x), x.product, x.url")
    rows, ms = await engine.gq(cy, {"sid": sid, "top": top})
    nodes, links = {"you": {"id": "you", "label": "you", "kind": "user"}}, []
    for cid, ctext, w, eid, ename, ekind, p, xid, prod, url in rows:
        nodes[cid] = {"id": cid, "label": ctext, "kind": "criterion", "weight": w}
        links.append({"source": "you", "target": cid, "kind": "wants", "w": w})
        if eid:
            nodes[eid] = {"id": eid, "label": ename, "kind": ekind}
            links.append({"source": cid, "target": eid, "kind": "scored", "p": p})
        if xid is not None:
            xk = f"x{xid}"
            nodes[xk] = {"id": xk, "label": prod, "kind": "evidence", "url": url}
            links.append({"source": eid, "target": xk, "kind": "evidence"})
    uniq = {(l["source"], l["target"]): l for l in links}
    return JSONResponse({"nodes": list(nodes.values()), "links": list(uniq.values()), "cypher": cy, "ms": ms})


@app.get("/api/calendar.ics")
async def calendar(sid: str):
    from fastapi.responses import Response
    s = session(sid)
    fmt = "%Y%m%dT%H%M%S"
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//StratCon//GitHub Universe 2026//EN", "CALSCALE:GREGORIAN"]
    for i, item in enumerate(engine.plan(s)):
        esc = lambda t: t.replace("\\", "\\\\").replace(",", "\\,").replace(";", "\\;").replace("\n", " ")
        lines += ["BEGIN:VEVENT", f"UID:stratcon-{sid}-{i}@stratcon", f"DTSTAMP:{engine.datetime.utcnow().strftime(fmt)}Z",
                  f"DTSTART;TZID=America/Los_Angeles:{item['start'].strftime(fmt)}",
                  f"DTEND;TZID=America/Los_Angeles:{item['end'].strftime(fmt)}",
                  f"SUMMARY:{esc(item['title'])}", f"LOCATION:{esc(item['where'])}",
                  f"DESCRIPTION:{esc(item['why'][:300])}", "END:VEVENT"]
    lines.append("END:VCALENDAR")
    return Response("\r\n".join(lines) + "\r\n", media_type="text/calendar",
                    headers={"Content-Disposition": "attachment; filename=stratcon-universe-2026.ics"})


@app.post("/api/reset")
async def reset(req: Request):
    sid = (await req.json())["sid"]
    sessions.pop(sid, None)
    await engine.gq("MATCH (u:User {id:$sid}) DETACH DELETE u", {"sid": sid})
    return JSONResponse({"ok": True})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
