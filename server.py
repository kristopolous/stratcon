import asyncio
import json
import os

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

import engine

app = FastAPI(title="StratCon")
HERE = os.path.dirname(__file__)
sessions: dict[str, engine.Session] = {}


def session(sid: str) -> engine.Session:
    if sid not in sessions:
        sessions[sid] = engine.Session(sid)
    return sessions[sid]


@app.on_event("startup")
async def startup():
    await engine.load_entities()
    asyncio.create_task(engine.warmup())


@app.get("/")
async def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"))


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


@app.post("/api/reset")
async def reset(req: Request):
    sid = (await req.json())["sid"]
    sessions.pop(sid, None)
    await engine.gq("MATCH (u:User {id:$sid}) DETACH DELETE u", {"sid": sid})
    return JSONResponse({"ok": True})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
