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
    if getattr(s, "current", None) and not s.current.done():
        s.current.cancel()
        for t in s.tasks:
            t.cancel()
        s.tasks = []
        if s.tf_id and engine.config.TRUEFORGE_URL:
            try:
                await engine.tf_client().sessions.cancel(session_id=s.tf_id)
            except Exception:
                pass
        aborted = True
    else:
        aborted = False
    q: asyncio.Queue = asyncio.Queue()

    async def produce():
        try:
            if aborted:
                await q.put(engine.ev("aborted", text="Previous turn stopped; re-planning with what you just said."))
            async for e in engine.turn(s, body["text"]):
                await q.put(e)
        except asyncio.CancelledError:
            pass
        except Exception as x:
            await q.put(engine.ev("error", text=str(x)[:300]))
        finally:
            await q.put(None)

    s.current = asyncio.create_task(produce())

    async def stream():
        while True:
            e = await q.get()
            if e is None:
                break
            yield f"data: {json.dumps(e)}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/graph")
async def graph(sid: str):
    s = session(sid)
    top = [r["id"] for r in s.ranking()[:10] if r["score"] > 0]
    cy = ("MATCH (u:User {id:$sid})-[w:WANTS]->(c:Criterion) "
          "OPTIONAL MATCH (c)-[m:MAPS_TO]->(t:Topic) "
          "OPTIONAL MATCH (t)-[v:COVERS]->(e:Entity) WHERE e.id IN $top "
          "RETURN c.id, c.text, w.weight, t.id, t.name, m.p, e.id, e.name, e.kind")
    rows, ms = await engine.gq(cy, {"sid": sid, "top": top})
    cy2 = ("MATCH (a:Entity)-[:PRESENTED_BY|EMPLOYS]->(p:Person)-[r:WORKS_AT|PRESENTS]->(b:Entity) "
           "WHERE a.id IN $top AND b.id IN $top RETURN a.id, p.name, b.id, type(r)")
    rows2, ms2 = await engine.gq(cy2, {"top": top})
    cy3 = ("MATCH (x:Evidence)-[:ABOUT]->(e:Entity) WHERE e.id IN $top RETURN id(x), x.product, x.url, e.id")
    rows3, ms3 = await engine.gq(cy3, {"top": top})
    nodes, links = {"you": {"id": "you", "label": "you", "kind": "user"}}, []
    for cid, ctext, w, tid, tname, mp, eid, ename, ekind in rows:
        nodes[cid] = {"id": cid, "label": ctext, "kind": "criterion", "weight": w}
        links.append({"source": "you", "target": cid, "kind": "wants"})
        if tid:
            nodes["t:" + tid] = {"id": "t:" + tid, "label": tname.split(",")[0], "kind": "topic"}
            links.append({"source": cid, "target": "t:" + tid, "kind": "maps", "p": mp})
        if eid:
            nodes[eid] = {"id": eid, "label": ename, "kind": ekind}
            links.append({"source": "t:" + tid, "target": eid, "kind": "covers"})
    for a, pname, b, rtype in rows2:
        pid = "p:" + pname
        nodes[pid] = {"id": pid, "label": pname, "kind": "person"}
        for eid in (a, b):
            e = engine.ENTITIES.get(eid, {})
            nodes.setdefault(eid, {"id": eid, "label": e.get("name", eid), "kind": e.get("kind", "")})
        links += [{"source": a, "target": pid, "kind": "person"}, {"source": pid, "target": b, "kind": "person"}]
    for xid, prod, url, eid in rows3:
        if eid in nodes:
            nodes[f"x{xid}"] = {"id": f"x{xid}", "label": prod, "kind": "evidence", "url": url}
            links.append({"source": eid, "target": f"x{xid}", "kind": "evidence"})
    uniq = {(l["source"], l["target"]): l for l in links}
    return JSONResponse({"nodes": list(nodes.values()), "links": list(uniq.values()),
                         "cypher": cy, "ms": ms + ms2 + ms3})


@app.get("/api/entity")
async def entity(sid: str, id: str):
    s = session(sid)
    e = engine.ENTITIES.get(id)
    if not e:
        return JSONResponse({"error": "not found"}, status_code=404)
    queries = []
    cy = "MATCH (e:Entity {id:$id})-[:LISTED_ON]->(src:Source) RETURN src.url, src.product, src.ms, src.bytes"
    rows, ms = await engine.gq(cy, {"id": id})
    sources = [{"url": u, "product": p, "ms": m, "bytes": b} for u, p, m, b in rows]
    queries.append({"cypher": cy, "ms": ms})
    cy = ("MATCH (e:Entity {id:$id}) OPTIONAL MATCH (x:Evidence)-[:ABOUT]->(e) OPTIONAL MATCH (x)-[:INFORMS]->(c:Criterion) "
          "RETURN x.url, x.product, x.snippet, c.text, x.calls")
    rows, ms = await engine.gq(cy, {"id": id})
    evidence = [{"url": u, "product": p, "snippet": sn, "criterion": ct, "calls": json.loads(cl) if cl else []}
                for u, p, sn, ct, cl in rows if u]
    queries.append({"cypher": cy, "ms": ms})
    related = []
    if e["kind"] == "sponsor":
        cy = ("MATCH (p:Person)-[:PRESENTS]->(t:Entity {kind:'talk'}) WHERE toLower(p.org) CONTAINS toLower($org) "
              "RETURN p.name, p.title, t.id, t.name, t.when, t.room LIMIT 12")
        rows, ms = await engine.gq(cy, {"org": e["name"].split(",")[0]})
        related = [{"label": f"{pn} ({pt}) presents", "id": ti, "name": tn, "when": tw, "room": tr} for pn, pt, ti, tn, tw, tr in rows]
        queries.append({"cypher": cy, "ms": ms})
    elif e["kind"] in ("speaker", "person"):
        cy = "MATCH (e:Entity {id:$id})-[:PRESENTS]->(t:Entity) RETURN t.id, t.name, t.when, t.room"
        rows, ms = await engine.gq(cy, {"id": id})
        related = [{"label": "presents", "id": ti, "name": tn, "when": tw, "room": tr} for ti, tn, tw, tr in rows]
        queries.append({"cypher": cy, "ms": ms})
    elif e["kind"] == "talk":
        cy = ("MATCH (p:Person)-[:PRESENTS]->(t:Entity {id:$id}) OPTIONAL MATCH (p)-[:PRESENTS]->(o:Entity) WHERE o.id <> $id "
              "RETURN p.name, p.title, p.org, collect(DISTINCT o.name)[0..3]")
        rows, ms = await engine.gq(cy, {"id": id})
        related = [{"label": f"{pn}, {pt} · {po}", "also": also} for pn, pt, po, also in rows]
        queries.append({"cypher": cy, "ms": ms})
    scores = [{"criterion": c["text"], "weight": c["weight"], **({"p": s.cells[(id, cid)]["p"],
               "evidence": s.cells[(id, cid)].get("evidence")} if (id, cid) in s.cells else {})}
              for cid, c in s.criteria.items()]
    score = next((r["score"] for r in s.ranking(everything=True) if r["id"] == id), None)
    return JSONResponse({"entity": e, "score": score, "scores": scores, "evidence": evidence, "related": related,
                         "sources": sources, "queries": queries, "calls": s.calls.get(id, [])})


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
