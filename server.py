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
    s.pins = [p for p in body.get("pins", []) if p in engine.ENTITIES]
    if getattr(s, "current", None) and not s.current.done():
        s.current.cancel()
        for t in s.tasks:
            t.cancel()
        s.tasks = []
        if s.tf_id and engine.config.TRUEFORGE_URL:
            try:
                await asyncio.wait_for(engine.tf_client().sessions.cancel(session_id=s.tf_id), 3)
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

    s.current = mine = asyncio.create_task(produce())

    async def stream():
        try:
            while True:
                e = await q.get()
                if e is None:
                    break
                yield f"data: {json.dumps(e)}\n\n"
        finally:
            if s.current is mine and not mine.done():
                mine.cancel()
                for t in s.tasks:
                    t.cancel()
                s.tasks = []

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
        cy = "MATCH (p {id:$id})-[:WORKS_AT]->(s:Entity) RETURN s.id, s.name, s.tier"
        rows, ms = await engine.gq(cy, {"id": id})
        related += [{"label": "at the booth of", "id": si, "name": sn, "when": f"{st} sponsor", "room": "expo hall"}
                    for si, sn, st in rows]
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


@app.get("/api/enrich")
async def enrich(id: str):
    """Who is this person? Cached in the graph; otherwise one Bright Data SERP lookup for their LinkedIn."""
    import brightdata
    e = engine.ENTITIES.get(id)
    if not e or e["kind"] not in ("person", "speaker"):
        return JSONResponse({"error": "not a person"}, status_code=400)
    rows, _ = await engine.gq("MATCH (p {id:$id}) RETURN p.linkedin, p.headline, p.profile_ms, p.socials, p.li_profile, "
                              "p.li_snapshot, p.li_ms, p.photo, p.photo_src", {"id": id})
    if rows and (rows[0][0] or rows[0][3] is not None):
        out = {"linkedin": rows[0][0] or "", "headline": rows[0][1] or "", "cached": True, "ms": rows[0][2],
               "other": json.loads(rows[0][3]) if rows[0][3] else []}
        if rows[0][7]:
            src = rows[0][8] or "LinkedIn"
            out["photo"] = {"url": rows[0][7], "source": src, "product": "graph memory"}
        elif out["other"]:
            gh = await _github_photo(id, [o["url"] for o in out["other"]])
            if gh:
                out["photo"] = gh
        if rows[0][4]:
            out["scraper"] = {"product": "Web Scraper API · LinkedIn people profiles", "status": "ready", "cached": True,
                              "snapshot": rows[0][5], "ms": rows[0][6], "profile": json.loads(rows[0][4])}
        elif out["linkedin"]:
            out["scraper"] = await _linkedin(id, out["linkedin"])
        return JSONResponse(out)
    q1 = f"\"{e['name']}\" {e.get('org', '')} linkedin"
    q2 = f"\"{e['name']}\" (site:github.com OR site:x.com OR site:bsky.app OR site:mastodon.social)"
    r1, r2 = await asyncio.gather(brightdata.serp(q1), brightdata.serp(q2))
    hit = next((x for x in r1["results"] if "linkedin.com/in/" in x["link"]), None)
    if not hit:
        q3 = f"site:linkedin.com/in \"{e['name']}\" {e.get('org', '')}"
        r3 = await brightdata.serp(q3)
        hit = next((x for x in r3["results"] if "linkedin.com/in/" in x["link"]), None)
        r1 = {**r1, "ms": r1["ms"] + r3["ms"]}
        q1 = q1 + "  →  " + q3
    last = e["name"].split()[-1].lower()
    socials = [x for x in r2["results"] if last in (x["title"] + x["link"] + x["description"]).lower()][:3]
    out = {"queries": [{"q": q1, "ms": r1["ms"], "bytes": r1["bytes"]}, {"q": q2, "ms": r2["ms"], "bytes": r2["bytes"]}],
           "product": "SERP API", "cached": False,
           "linkedin": hit["link"] if hit else "", "headline": (hit["title"] + " — " + hit["description"])[:300] if hit else "",
           "other": [{"title": x["title"], "url": x["link"], "snippet": x["description"][:200]} for x in socials]}
    if r1.get("status") == 200 and r2.get("status") == 200:
        await engine.gq("MATCH (p {id:$id}) SET p.linkedin=$l, p.headline=$h, p.socials=$so, p.profile_ms=$ms",
                        {"id": id, "l": out["linkedin"], "h": out["headline"], "so": json.dumps(out["other"]),
                         "ms": max(r1["ms"], r2["ms"])})
    gh = await _github_photo(id, [x["url"] for x in out["other"]])
    if gh:
        out["photo"] = gh
    if hit:
        out["scraper"] = await _linkedin(id, hit["link"].split("?")[0])
    return JSONResponse(out)


LI_JOBS: dict[str, dict] = {}


async def _github_photo(id: str, links: list[str]) -> dict | None:
    """Bright Data Web Unlocker on the person's GitHub profile; the avatar is public even when LinkedIn's isn't.
    Only a profile whose page names this person is accepted."""
    import re as _re
    import brightdata
    e = engine.ENTITIES.get(id) or {}
    name = (e.get("name") or "").lower()
    last = name.split()[-1] if name else ""
    users = []
    for l in links:
        l = l.strip()
        if not l.startswith("http"):
            l = "https://" + l.lstrip("/")
        m = _re.match(r"https?://(?:www\.)?github\.com/([A-Za-z0-9-]+)/?(?:[?#].*)?$", l)
        if m and m.group(1).lower() not in ("orgs", "topics", "features", "about", "github", "sponsors", "settings") \
                and m.group(1) not in users:
            users.append(m.group(1))
    for user in users[:2]:
        try:
            status, html = await brightdata._request(f"https://github.com/{user}", None)
        except Exception:
            return None
        if status != 200 or (last and last not in html.lower()):
            continue
        m = _re.search(r"https://avatars\.githubusercontent\.com/u/\d+\?v=\d+", html)
        if not m:
            continue
        out = {"url": m.group(0) + "&s=200", "source": f"github.com/{user}", "product": "Web Unlocker"}
        await engine.gq("MATCH (p {id:$id}) SET p.photo = CASE WHEN p.photo IS NULL OR p.photo = '' THEN $u ELSE p.photo END, "
                        "p.photo_src = CASE WHEN p.photo IS NULL OR p.photo = '' OR p.photo = $u THEN $src ELSE p.photo_src END",
                        {"id": id, "u": out["url"], "src": out["source"]})
        if id in engine.ENTITIES and not engine.ENTITIES[id].get("logo"):
            engine.ENTITIES[id]["logo"] = out["url"]
        return out
    return None


LI_LOCKS: dict[str, asyncio.Lock] = {}


async def _linkedin(id: str, url: str) -> dict:
    """Bright Data Web Scraper API, LinkedIn people profiles: trigger once (resuming a snapshot saved in the graph
    after a restart), report progress until ready or failed, never trigger two scrapes for one person."""
    import time as _t
    import brightdata
    async with LI_LOCKS.setdefault(id, asyncio.Lock()):
        job = LI_JOBS.get(id)
        try:
            if not job:
                rows, _ = await engine.gq("MATCH (p {id:$id}) RETURN p.li_pending, p.li_pending_at", {"id": id})
                if rows and rows[0][0]:
                    job = LI_JOBS[id] = {"snapshot": rows[0][0], "started": (rows[0][1] or _t.time() * 1000) / 1000,
                                         "status": "running", "url": url}
                else:
                    sid = await brightdata.dataset_trigger(brightdata.LINKEDIN_PEOPLE, [{"url": url}])
                    job = LI_JOBS[id] = {"snapshot": sid, "started": _t.time(), "status": "running", "url": url}
                    await engine.gq("MATCH (p {id:$id}) SET p.li_pending=$s, p.li_pending_at=timestamp()",
                                    {"id": id, "s": sid})
            if job["status"] == "running" and _t.time() - job["started"] > 240 and not job.get("retried"):
                sid = await brightdata.dataset_trigger(brightdata.LINKEDIN_PEOPLE, [{"url": job["url"]}])
                job.update({"snapshot": sid, "started": _t.time(), "retried": True})
                await engine.gq("MATCH (p {id:$id}) SET p.li_pending=$s, p.li_pending_at=timestamp()", {"id": id, "s": sid})
            if job["status"] == "running":
                prog = await brightdata.snapshot_progress(job["snapshot"])
                st = prog.get("status")
                if prog.get("transient"):
                    pass
                elif st == "failed" or prog.get("errors") and not prog.get("records"):
                    job["status"], job["error"] = "failed", str(prog.get("error") or prog.get("errors"))[:200]
                elif st == "ready":
                    recs = await brightdata.snapshot_download(job["snapshot"])
                    rec = recs[0] if recs and isinstance(recs[0], dict) else {}
                    if not rec or rec.get("error") or not rec.get("name"):
                        job["status"], job["error"] = "failed", str(rec.get("error") or "no profile returned")[:200]
                    else:
                        photo = "" if rec.get("default_avatar") else (rec.get("avatar") or "")
                        comp = rec.get("current_company") or {}
                        job["profile"] = {"name": rec.get("name"), "photo": photo, "about": (rec.get("about") or "")[:400],
                                          "company": comp.get("name") if isinstance(comp, dict) else rec.get("current_company_name"),
                                          "city": rec.get("city"), "followers": rec.get("followers"),
                                          "url": rec.get("url") or url, "position": rec.get("position")}
                        job["status"] = "ready"
                        job["ms"] = int((_t.time() - job["started"]) * 1000)
                        await engine.gq(
                            "MATCH (p {id:$id}) SET p.li_profile=$j, p.li_snapshot=$sid, p.li_ms=$ms, p.li_pending=null, "
                            "p.photo = CASE WHEN $ph <> '' THEN $ph ELSE p.photo END, "
                            "p.photo_src = CASE WHEN $ph <> '' THEN 'LinkedIn' ELSE p.photo_src END",
                            {"id": id, "j": json.dumps(job["profile"]), "ph": photo, "sid": job["snapshot"], "ms": job["ms"]})
                        if photo and id in engine.ENTITIES:
                            engine.ENTITIES[id]["logo"] = photo
        except Exception as x:
            if job and job.get("snapshot"):  # transient: keep polling the same snapshot next time
                job["error"] = f"{type(x).__name__}: {str(x)[:160]}"
            else:
                job = {"snapshot": None, "started": _t.time(), "status": "failed",
                       "error": f"{type(x).__name__}: {str(x)[:160]}"}
                LI_JOBS[id] = job
        if job["status"] == "failed":
            await engine.gq("MATCH (p {id:$id}) SET p.li_pending=null", {"id": id})
    return {"product": "Web Scraper API · LinkedIn people profiles", "dataset": "gd_l1viktl72bvl7bjuj0",
            "snapshot": job.get("snapshot"), "status": job["status"], "elapsed": int(_t.time() - job["started"]),
            "profile": job.get("profile"), "ms": job.get("ms"), "error": job.get("error")}


@app.get("/api/topic")
async def topic(sid: str, id: str):
    cy = ("MATCH (t:Topic {id:$id})-[c:COVERS]->(e:Entity) "
          "RETURN t.name, e.id, e.name, e.kind, c.p ORDER BY c.p DESC LIMIT 200")
    rows, ms = await engine.gq(cy, {"id": id})
    total, _ = await engine.gq("MATCH (:Topic {id:$id})-[:COVERS]->(e:Entity) RETURN count(e)", {"id": id})
    s = session(sid)
    scores = {r["id"]: r["score"] for r in s.ranking(everything=True)}
    return JSONResponse({"name": rows[0][0] if rows else id, "cypher": cy, "ms": ms, "total": total[0][0] if total else len(rows),
                         "entities": [{"id": i, "name": n, "kind": k, "p": p, "score": scores.get(i, 0)} for _, i, n, k, p in rows]})


@app.get("/api/criterion")
async def criterion(sid: str, id: str):
    s = session(sid)
    c = s.criteria.get(id)
    cy = "MATCH (c:Criterion {id:$id})-[m:MAPS_TO]->(t:Topic) RETURN t.id, t.name, m.p ORDER BY m.p DESC"
    rows, ms = await engine.gq(cy, {"id": id})
    best = sorted(((eid, cell["p"]) for (eid, cid), cell in s.cells.items() if cid == id), key=lambda x: -x[1])[:15]
    return JSONResponse({"text": c["text"] if c else id, "weight": c["weight"] if c else None, "cypher": cy, "ms": ms,
                         "topics": [{"id": t, "name": n, "p": p} for t, n, p in rows],
                         "best": [{"id": e, "name": engine.ENTITIES[e]["name"], "kind": engine.ENTITIES[e]["kind"], "p": p}
                                  for e, p in best if e in engine.ENTITIES]})


@app.get("/api/sessions")
async def sessions_list():
    cy = ("MATCH (u:User) WHERE u.title IS NOT NULL OPTIONAL MATCH (u)-[:WANTS]->(c:Criterion) "
          "OPTIONAL MATCH (t:Turn)-[:IN]->(u) "
          "RETURN u.id, u.title, u.updated, count(DISTINCT c), count(DISTINCT t) ORDER BY u.updated DESC LIMIT 15")
    rows, ms = await engine.gq(cy)
    return JSONResponse({"cypher": cy, "ms": ms, "sessions": [
        {"id": i, "title": t, "updated": u, "criteria": nc, "turns": nt} for i, t, u, nc, nt in rows if nt]})


@app.get("/api/session/load")
async def session_load(sid: str):
    """Rebuild a past session entirely from the graph: no agent, no decision model, no fetches."""
    live = sessions.get(sid)
    s = engine.Session(sid)
    if live and getattr(live, "current", None) and not live.current.done():
        s = live
    else:
        sessions[sid] = s
    cy1 = ("MATCH (u:User {id:$sid}) OPTIONAL MATCH (u)-[w:WANTS]->(c:Criterion) "
           "RETURN u.scope, u.pins, u.now, c.id, c.text, w.weight")
    rows, ms1 = await engine.gq(cy1, {"sid": sid})
    for scope, pins, now, cid, text, w in ([] if s is live else rows):
        s.scope = scope or "all"
        s.pins = [p for p in (pins or []) if p in engine.ENTITIES]
        if now:
            from datetime import datetime
            s.now = datetime.fromisoformat(now)
        if cid:
            s.criteria[cid] = {"id": cid, "text": text, "weight": w}
    if s is live:
        cells = [{"e": e, "c": c, "p": round(v["p"], 4), "src": v.get("src"), "evidence": v.get("evidence"),
                  "calls": v.get("calls")} for (e, c), v in s.cells.items() if c in s.criteria]
        cy2, ms2 = "", 0
    else:
        cells, _, cy2, ms2 = await engine.recall(s, list(s.criteria)) if s.criteria else ([], None, "", 0)
    cy3 = "MATCH (t:Turn)-[:IN]->(:User {id:$sid}) RETURN t.text, t.reply ORDER BY t.at"
    turns, ms3 = await engine.gq(cy3, {"sid": sid})
    if s is not live:
        for a, b in turns:
            s.history += [{"role": "user", "content": a or ""}, {"role": "assistant", "content": b or ""}]
    return JSONResponse({"sid": sid, "criteria": list(s.criteria.values()), "scope": s.scope, "pins": s.pins,
                         "cells": cells, "rows": s.ranking(), "turns": [{"text": a, "reply": b} for a, b in turns],
                         "queries": [{"cypher": c, "ms": m} for c, m in ((cy1, ms1), (cy2, ms2), (cy3, ms3)) if c]})


@app.post("/api/pins")
async def save_pins(req: Request):
    body = await req.json()
    s = session(body["sid"])
    rev = int(body.get("rev", 0))
    if rev and rev < getattr(s, "pin_rev", 0):
        return JSONResponse({"ok": False, "stale": True, "pins": s.pins})
    s.pin_rev = rev
    s.pins = [p for p in body.get("pins", []) if p in engine.ENTITIES]
    await engine.gq("MERGE (u:User {id:$sid}) SET u.pins = $pins", {"sid": body["sid"], "pins": s.pins})
    return JSONResponse({"ok": True, "pins": s.pins})


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
