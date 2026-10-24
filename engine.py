"""StratCon engine. A turn is a stream of events, each one a real call that happened."""
import asyncio
import json
import re
import time
from datetime import datetime, timedelta

import httpx
from falkordb import FalkorDB

import brightdata
import config

graph = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)

GAP_LO, GAP_HI = 0.25, 0.75
MAX_GAPS = 4
MAX_Q = 6
RETRIEVE_PER_CRIT = 5
SHOW_ROWS = 8
TOP_FOR_GAPS = 6
STOP = set("does with that this from have into your their about which what when helps help more than they them".split())

STRATEGIES = {
    "web_search": ("SERP API", "Google search for pages that discuss this exact question. 10-30s. Best when the answer "
                   "is likely on third-party pages, reviews, or docs."),
    "read_site": ("Web Unlocker", "Read the organization's official website (domain resolved by search if unknown). "
                  "2-5s. Best for capabilities a vendor states about its own product."),
    "render_browser": ("Browser API", "Load the official website in a full remote browser. Slowest, ~30s. Only worth it "
                       "when a plain read came back empty or JavaScript-only."),
    "people_search": ("SERP API", "Search for this person's own work on the topic: talks, courses, blog posts, repos. "
                      "Best for finding out who a person is and what they are known for."),
}


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:48]


def _reconnect():
    global graph
    graph = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)


async def gq(cypher: str, params: dict | None = None):
    t = time.time()
    try:
        res = await asyncio.to_thread(graph.query, cypher, params or {})
    except Exception as x:
        if "pipe" not in str(x).lower() and "connection" not in str(x).lower() and "closed" not in str(x).lower():
            raise
        await asyncio.to_thread(_reconnect)
        res = await asyncio.to_thread(graph.query, cypher, params or {})
    return res.result_set, int((time.time() - t) * 1000)


def talk_window(e: dict):
    m = re.match(r"\w+, Oct (\d+) · (\d{1,2}:\d{2} [AP]M) - (\d{1,2}:\d{2} [AP]M)", e.get("when") or "")
    if not m:
        return None
    day = int(m.group(1))
    start = datetime.strptime(f"2026-10-{day:02d} {m.group(2)}", "%Y-%m-%d %I:%M %p")
    end = datetime.strptime(f"2026-10-{day:02d} {m.group(3)}", "%Y-%m-%d %I:%M %p")
    return start, end


def plan(session: "Session", talks: int = 6, booths: int = 4) -> list[dict]:
    """Greedy non-overlapping schedule from the current ranking, plus one expo-hall block for top booths."""
    ranking = [r for r in session.ranking(everything=True) if r["score"] > 0.2]
    chosen = []
    for r in ranking:
        e = ENTITIES[r["id"]]
        w = talk_window(e) if e["kind"] == "talk" else None
        if w and all(w[1] <= c["start"] or w[0] >= c["end"] for c in chosen) and len(chosen) < talks:
            chosen.append({"title": e["name"], "start": w[0], "end": w[1], "where": e.get("room", ""),
                           "why": e.get("speakers", ""), "score": r["score"]})
    top_booths = [ENTITIES[r["id"]] for r in ranking if ENTITIES[r["id"]]["kind"] == "sponsor"][:booths]
    if top_booths:
        slot = max(datetime(2026, 10, 28, 10, 0), session.now or datetime.min)
        slot = slot.replace(minute=(slot.minute // 15) * 15, second=0, microsecond=0)
        while any(not (slot + timedelta(minutes=45) <= c["start"] or slot >= c["end"]) for c in chosen):
            slot += timedelta(minutes=15)
        chosen.append({"title": "Expo hall: " + ", ".join(b["name"] for b in top_booths), "start": slot,
                       "end": slot + timedelta(minutes=45), "where": "Partner booths, Fort Mason Center",
                       "why": "; ".join(f"{b['name']}: {b['description'][:80]}" for b in top_booths), "score": 0})
    return sorted(chosen, key=lambda c: c["start"])


def ev(type_: str, **kw) -> dict:
    return {"type": type_, "t": time.time(), **kw}


ENTITIES: dict[str, dict] = {}
ACTIVE = None


async def load_entities():
    rows, _ = await gq("MATCH (e:Entity) RETURN e.id, e.kind, e.name, e.tier, e.description, e.url, e.org, "
                       "e.when, e.room, e.logo, e.speakers")
    ENTITIES.clear()
    for i, k, n, t, d, u, o, w, r, lg, sp in rows:
        ENTITIES[i] = {"id": i, "kind": k, "name": n, "tier": t, "description": d, "url": u or "", "org": o or "",
                       "when": w or "", "room": r or "", "logo": lg or "", "speakers": sp or ""}


async def systemone(model: str, state: str, questions: dict) -> tuple[dict, int]:
    t = time.time()
    async with httpx.AsyncClient(timeout=180) as c:
        r = await c.post(config.SYSTEMONE_URL, json={"model": model, "state": state, "questions": questions})
        if r.status_code != 200:
            raise RuntimeError(f"systemone {r.status_code}: {r.text[:300]}")
        return r.json()["answers"], int((time.time() - t) * 1000)


async def warmup():
    q = {"w": {"type": "noul", "instructions": "ready?", "criteria": {"false": "no", "true": "yes"}}}
    await asyncio.gather(*(systemone(m, "warmup", q) for m in {config.SCORE_MODEL, config.ROUTER_MODEL}),
                         return_exceptions=True)


def noul(instr: str) -> dict:
    return {"type": "noul", "instructions": instr,
            "criteria": {"false": "No, or no evidence of it", "true": "Yes, clearly"}}


class Session:
    def __init__(self, sid: str):
        self.sid = sid
        self.criteria: dict[str, dict] = {}
        self.scope = "all"
        self.cells: dict[tuple[str, str], dict] = {}
        self.tried: dict[tuple[str, str], list] = {}
        self.history: list[dict] = []
        self.inbox: asyncio.Queue = asyncio.Queue()
        self.ops: list[dict] = []
        self.tf_id: str | None = None
        self.now: datetime | None = None

    def entities(self, everything: bool = False):
        pool = ENTITIES.values() if everything else self._scoped()
        return [e for e in pool if not (self.now and e["kind"] == "talk" and talk_window(e)
                                        and talk_window(e)[0] < self.now)]

    def _scoped(self):
        if self.scope == "sponsors":
            return [e for e in ENTITIES.values() if e["kind"] == "sponsor"]
        if self.scope == "speakers":
            return [e for e in ENTITIES.values() if e["kind"] == "speaker"]
        if self.scope == "talks":
            return [e for e in ENTITIES.values() if e["kind"] == "talk"]
        return list(ENTITIES.values())

    def score(self, eid: str) -> float:
        pos = sum(c["weight"] for c in self.criteria.values() if c["weight"] > 0) or 1.0
        s = 0.0
        for cid, c in self.criteria.items():
            cell = self.cells.get((eid, cid))
            if cell:
                s += c["weight"] * cell["p"]
        return s / pos

    def ranking(self, everything: bool = False):
        rows = [{"id": e["id"], "score": round(self.score(e["id"]), 4)} for e in self.entities(everything)]
        return sorted(rows, key=lambda r: -r["score"])


AGENT_PROMPT = """You maintain the recommendation rubric for one attendee of GitHub Universe 2026 \
(54 expo-hall sponsor booths, 157 talks and sessions, and featured speakers). The rubric is a short list of criteria. Each criterion is a \
yes/no-checkable statement about a sponsor or speaker, e.g. "Helps automatically test web apps". \
Weight 0.1 to 1.0 says how much it matters; use a negative weight (-1.0 to -0.1) for things to avoid.

Given the current rubric and the attendee's new message, output ONLY a JSON object:
{"ops": [...], "reply": "<one short sentence back to the attendee>"}
ops are any of:
  {"op":"add","text":"<criterion>","weight":0.7}
  {"op":"remove","id":"<criterion id>"}
  {"op":"reweight","id":"<criterion id>","weight":0.9}
  {"op":"scope","value":"all" | "sponsors" | "speakers" | "talks"}
  {"op":"clock","value":"2026-10-28T14:00"}   (when the attendee says what day/time it is at the conference; \
the conference runs Oct 28-29, 2026)
  {"op":"calendar"}   (when the attendee asks to export, save or add their plan to a calendar)
Rules: at most 5 criteria in total. "Which talks/sessions" means scope talks; "which booths/sponsors" means \
scope sponsors; "which speakers" means scope speakers; "everything" means scope all. When the attendee changes their mind, edit existing criteria instead of \
piling on: "matters less" / "more important" means reweight; "forget" / "drop" / "no longer" means remove. \
If they bring back something from earlier in the conversation, add it back with the same wording as before. Phrase each criterion concretely in 8 words or fewer so it can be checked against a company description."""


async def agent(session: Session, text: str) -> tuple[dict, int]:
    rubric = [{"id": c["id"], "text": c["text"], "weight": c["weight"]} for c in session.criteria.values()]
    msgs = [{"role": "system", "content": AGENT_PROMPT}]
    for h in session.history[-6:]:
        msgs.append(h)
    msgs.append({"role": "user", "content": f"Current rubric: {json.dumps(rubric)}\nScope: {session.scope}\n"
                                            f"Attendee says: {text}"})
    t = time.time()
    headers = {"Authorization": f"Bearer {config.LLM_API_KEY}"} if config.LLM_API_KEY else {}
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.post(f"{config.LLM_BASE_URL}/chat/completions", headers=headers,
                         json={"model": config.LLM_MODEL, "messages": msgs, "stream": False})
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
    m = re.search(r"\{.*\}", content, re.S)
    out = json.loads(m.group(0)) if m else {"ops": [], "reply": content.strip()[:200]}
    session.history += [{"role": "user", "content": text}, {"role": "assistant", "content": json.dumps(out)}]
    return out, int((time.time() - t) * 1000)


TF_PROMPT = AGENT_PROMPT.split("Given the current rubric")[0] + """Edit the rubric only through the stratcon tools: \
add_criterion, reweight, remove_criterion, set_scope, set_clock (when they say what day/time it is at the \
conference, Oct 28-29 2026), export_calendar (when they want their plan in a calendar); call get_rubric if you \
need criterion ids. Make every edit \
the attendee's message implies, then reply with one short sentence. Rules: at most 5 criteria. "matters less" / \
"more important" means reweight; "forget" / "drop" / "no longer" means remove. If they bring back something from \
earlier, add it back with the same wording as before. Phrase each criterion in 8 words or fewer."""

_tf = None


def tf_client():
    global _tf
    if _tf is None:
        from trueforge_sdk import AsyncTrueForge
        _tf = AsyncTrueForge(base_url=config.TRUEFORGE_URL, token=config.TRUEFORGE_TOKEN or None, timeout=180)
    return _tf


def _text(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(getattr(p, "text", "") or (p.get("text", "") if isinstance(p, dict) else "") for p in content)
    return str(content)


async def agent_trueforge(session: "Session", text: str):
    global ACTIVE
    import inspect
    import trueforge_sdk as t
    c = tf_client()
    t0 = time.time()
    if not session.tf_id:
        spec = t.AgentSpec(model=t.Model(name=config.TRUEFORGE_MODEL), instructions=TF_PROMPT,
                           mcp_servers=[t.McpServer(name=config.MCP_NAME, enable_tools=["@all"])])
        r = await c.sessions.create(agent=t.SessionAgentSpecBody(spec=spec), metadata={"stratcon_sid": session.sid})
        session.tf_id = getattr(getattr(r, "data", r), "id")
        yield ev("tf_session", id=session.tf_id, ms=int((time.time() - t0) * 1000))
    session.ops = []
    ACTIVE = session
    rubric = [{"id": k["id"], "text": k["text"], "weight": k["weight"]} for k in session.criteria.values()]
    reply, status = "", ""
    try:
        res = c.sessions.create_turn_stream(
            session_id=session.tf_id,
            input=[t.UserMessage(type="user.message",
                                 content=f"Current rubric: {json.dumps(rubric)}\nScope: {session.scope}\nAttendee says: {text}")])
        if inspect.isawaitable(res):
            res = await res
        async for e in res:
            kind = getattr(e, "type", "")
            if kind == "mcp.initialize":
                yield ev("tf_mcp", servers=[getattr(m, "name", "") for m in (e.mcp_servers or [])])
            elif kind == "model.message":
                if _text(e.content).strip():
                    reply = _text(e.content).strip()
                for tc in e.tool_calls or []:
                    yield ev("tf_tool_call", id=tc.id, name=tc.function.name, args=tc.function.arguments,
                             model=getattr(e, "model", None))
            elif kind == "tool.response":
                yield ev("tf_tool_result", id=e.tool_call_id, content=_text(e.content)[:300])
            elif kind == "turn.done":
                status = getattr(e.state, "status", "")
            while not session.inbox.empty():
                yield session.inbox.get_nowait()
    finally:
        ACTIVE = None
    while not session.inbox.empty():
        yield session.inbox.get_nowait()
    yield ev("agent", phase="done", model=config.TRUEFORGE_MODEL, via="TrueForge", status=status,
             ms=int((time.time() - t0) * 1000), ops=session.ops, reply=reply)


async def apply_ops(session: Session, ops: list[dict]):
    events = []
    for op in ops:
        kind = op.get("op")
        if kind == "add" and op.get("text"):
            cid = slug(op["text"])
            w = max(-1.0, min(1.0, float(op.get("weight", 0.5))))
            session.criteria[cid] = {"id": cid, "text": op["text"].strip(), "weight": w}
            cy = ("MERGE (u:User {id:$sid}) MERGE (c:Criterion {id:$cid}) ON CREATE SET c.text=$text "
                  "MERGE (u)-[r:WANTS]->(c) SET r.weight=$w")
            _, ms = await gq(cy, {"sid": session.sid, "cid": cid, "text": op["text"], "w": w})
            events.append(ev("graph", op="add", cypher=cy, ms=ms, crit=cid))
        elif kind == "remove" and op.get("id") in session.criteria:
            cid = op["id"]
            session.criteria.pop(cid)
            cy = "MATCH (:User {id:$sid})-[r:WANTS]->(:Criterion {id:$cid}) DELETE r"
            _, ms = await gq(cy, {"sid": session.sid, "cid": cid})
            events.append(ev("graph", op="remove", cypher=cy, ms=ms, crit=cid))
        elif kind == "reweight" and op.get("id") in session.criteria:
            cid = op["id"]
            w = max(-1.0, min(1.0, float(op.get("weight", 0.5))))
            events.append(ev("reweight", crit=cid, before=session.criteria[cid]["weight"], after=w))
            session.criteria[cid]["weight"] = w
            cy = "MATCH (:User {id:$sid})-[r:WANTS]->(:Criterion {id:$cid}) SET r.weight=$w"
            _, ms = await gq(cy, {"sid": session.sid, "cid": cid, "w": w})
            events.append(ev("graph", op="reweight", cypher=cy, ms=ms, crit=cid))
        elif kind == "scope" and op.get("value") in ("all", "sponsors", "speakers", "talks"):
            session.scope = op["value"]
        elif kind == "clock" and op.get("value"):
            try:
                session.now = datetime.fromisoformat(str(op["value"])[:16])
                events.append(ev("clock", now=session.now.isoformat(), label=session.now.strftime("%a %b %d, %-I:%M %p"),
                                 hidden=sum(1 for e in session._scoped() if e not in session.entities())))
            except ValueError:
                pass
        elif kind == "calendar":
            items = plan(session)
            events.append(ev("calendar", url=f"/api/calendar.ics?sid={session.sid}",
                             items=[{"title": i["title"], "start": i["start"].strftime("%a %-I:%M %p"),
                                     "where": i["where"]} for i in items]))
    return events


def keywords(*texts: str) -> list[str]:
    words = re.findall(r"[a-z0-9]+", " ".join(texts).lower())
    return [w for w in dict.fromkeys(words) if len(w) > 3 and w not in STOP]


def passages(text: str, kws: list[str], n: int = 3, maxlen: int = 700) -> str:
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", " ", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    parts = [re.sub(r"\s+", " ", p).strip(" -*#>|\t") for p in re.split(r"\n", text)]
    parts = [p for p in parts if 60 <= len(p) <= 600 and len(p.split()) >= 8]
    ranked = sorted(((sum(p.lower().count(k) for k in kws), p) for p in parts), key=lambda x: -x[0])
    out = " … ".join(p for sc, p in ranked[:n] if sc > 0)
    return out[:maxlen]


async def score_new(session: Session, crits: list[str], only: list[dict] | None = None):
    """Shortlist with one choice pass over everything, then batch-score the shortlist cell by cell."""
    if only is not None:
        async for e in score_cells(session, crits, only):
            yield e
        return
    ents = session.entities()
    rel: dict[str, float] = {e["id"]: 0.0 for e in ents}
    keep: set[str] = set()
    opts = {e["id"]: f"{e['name']}: {e['description'][:160]}" for e in ents}
    for cid in crits:
        text = session.criteria[cid]["text"]
        yield ev("retrieval", crit=cid, model=config.ROUTER_MODEL, phase="start", n=len(opts))
        a, ms = await systemone(config.ROUTER_MODEL, "GitHub Universe 2026 sponsor booths, talks and featured speakers.",
                                {"pick": {"type": "choice", "instructions": f"Which one best satisfies: {text}?",
                                          "criteria": opts}})
        probs = sorted(a["pick"]["probabilities"].items(), key=lambda x: -x[1])
        for i, pr in probs:
            rel[i] += pr
        keep |= {i for i, _ in probs[:RETRIEVE_PER_CRIT]}
        for kind in ("sponsor", "talk", "speaker"):
            keep |= {i for i, _ in [x for x in probs if ENTITIES[x[0]]["kind"] == kind][:2]}
        yield ev("retrieval", crit=cid, model=config.ROUTER_MODEL, phase="done", ms=ms, n=len(opts),
                 top=[[ENTITIES[i]["name"], round(pr, 3)] for i, pr in probs[:6]])
    keep |= {r["id"] for r in session.ranking()[:TOP_FOR_GAPS] if r["score"] > 0}
    ents = sorted((e for e in ents if e["id"] in keep), key=lambda e: -rel[e["id"]])
    yield ev("candidates", total=len(rel), kept=len(ents), names=[e["name"] for e in ents])
    async for e in score_cells(session, crits, ents):
        yield e


async def score_cells(session: Session, crits: list[str], ents: list[dict]):
    pairs = [(e, cid) for e in ents for cid in crits if (e["id"], cid) not in session.cells]
    for start in range(0, len(pairs), MAX_Q):
        chunk = pairs[start:start + MAX_Q]
        idx = {}
        for e, _ in chunk:
            idx.setdefault(e["id"], (len(idx), e))
        state = "GitHub Universe 2026 expo hall and speakers:\n" + "\n".join(
            f"[{i}] {e['name']} ({e['tier']}): {e['description']}" for i, e in idx.values())
        qs = {f"{idx[e['id']][0]}|{cid}": noul(f"Does [{idx[e['id']][0]}] {e['name']} satisfy: {session.criteria[cid]['text']}?")
              for e, cid in chunk}
        yield ev("s1_call", model=config.SCORE_MODEL, questions=len(qs), entities=len(idx), phase="start")
        answers, ms = await systemone(config.SCORE_MODEL, state, qs)
        by_i = {i: e for i, e in idx.values()}
        cells = []
        for key, a in answers.items():
            i, cid = key.split("|", 1)
            e = by_i[int(i)]
            p = float(a["noul"])
            session.cells[(e["id"], cid)] = {"p": p, "src": "model"}
            cells.append({"e": e["id"], "c": cid, "p": round(p, 4), "src": "model"})
        yield ev("s1_call", model=config.SCORE_MODEL, questions=len(qs), entities=len(idx), ms=ms, phase="done")
        yield ev("cells", cells=cells)
        yield ev("rank", rows=session.ranking())
        cy = ("UNWIND $rows AS r MATCH (e:Entity {id:r.e}), (c:Criterion {id:r.c}) "
              "MERGE (e)-[s:SCORED]->(c) SET s.p=r.p, s.model=$m, s.ev=0")
        _, gms = await gq(cy, {"rows": cells, "m": config.SCORE_MODEL})
        yield ev("graph", op="scored", cypher=cy, ms=gms, n=len(cells))


async def recall(session: Session, crits: list[str]):
    cy = ("MATCH (e:Entity)-[s:SCORED]->(c:Criterion) WHERE c.id IN $ids "
          "OPTIONAL MATCH (x:Evidence)-[:ABOUT]->(e) WHERE (x)-[:INFORMS]->(c) "
          "RETURN e.id, c.id, s.p, s.ev, x.product, x.url, x.snippet")
    rows, ms = await gq(cy, {"ids": crits})
    cells, have = [], set()
    for eid, cid, p, evn, prod, url, snip in rows:
        if eid not in ENTITIES:
            continue
        cell = {"p": float(p), "src": "memory"}
        if prod:
            cell["evidence"] = {"product": prod, "url": url, "snippet": snip}
        session.cells[(eid, cid)] = cell
        have.add((eid, cid))
        cells.append({"e": eid, "c": cid, "p": round(float(p), 4), "src": "memory", "evidence": cell.get("evidence")})
    return cells, have, cy, ms


_site_locks: dict[str, asyncio.Lock] = {}


async def resolve_site(e: dict):
    async with _site_locks.setdefault(e["id"], asyncio.Lock()):
        if e["url"]:
            return None, e["url"]
        r = await brightdata.serp(f"{e['name']} official website")
        skip = ("linkedin.", "wikipedia.", "crunchbase.", "github.com", "x.com", "twitter.", "youtube.", "githubuniverse")
        token = re.sub(r"[^a-z0-9]", "", e["name"].lower().split(",")[0].replace("inc", ""))[:8]
        hosts = []
        for res in r["results"]:
            m = re.match(r"https?://([^/]+)", res["link"])
            if m and not any(s in res["link"] for s in skip):
                hosts.append(m.group(1))
        best = next((h for h in hosts if token and token in h.replace("-", "").replace(".", "")), "")
        return r, (f"https://{best}" if best else "")


async def route(session: Session, gaps: list[dict]):
    qs = {}
    for i, g in enumerate(gaps):
        e = ENTITIES[g["e"]]
        tried = session.tried.get((g["e"], g["c"]), [])
        avail = [s for s in STRATEGIES if s not in [t["strategy"] for t in tried]]
        if e["kind"] == "speaker":
            avail = [s for s in avail if s in ("web_search", "people_search")]
        elif e["kind"] == "talk":
            avail = [s for s in avail if s == "web_search"]
        else:
            avail = [s for s in avail if s != "people_search"]
            if not any(t["strategy"] == "read_site" for t in tried):
                avail = [s for s in avail if s != "render_browser"]
        g["avail"] = avail
        if len(avail) > 1:
            tried_txt = "; ".join(f"{t['strategy']} -> {t['outcome']}" for t in tried) or "nothing yet"
            qs[str(i)] = {"type": "choice",
                          "instructions": (f"Pick the cheapest way to find out whether {e['name']} ({e['kind']}, "
                                           f"{e['tier']}) satisfies: \"{session.criteria[g['c']]['text']}\". "
                                           f"Current probability {g['p']:.2f}. What we know: {e['description']} "
                                           f"Official website: {e['url'] or 'unknown'}. Already tried: {tried_txt}."),
                          "criteria": {s: STRATEGIES[s][1] for s in avail}}
    answers, ms = ({}, 0)
    if qs:
        answers, ms = await systemone(config.ROUTER_MODEL, "Route evidence gathering across Bright Data products. "
                                      "Prefer the cheapest strategy likely to resolve the uncertainty.", qs)
    for i, g in enumerate(gaps):
        a = answers.get(str(i))
        if a:
            g["probs"] = {k: round(v, 3) for k, v in a["probabilities"].items()}
            g["strategy"] = a["choice"]
        elif g["avail"]:
            g["probs"] = {g["avail"][0]: 1.0}
            g["strategy"] = g["avail"][0]
        else:
            g["strategy"] = None
    return ms


async def execute(session: Session, g: dict, out: asyncio.Queue):
    e = ENTITIES[g["e"]]
    crit = session.criteria[g["c"]]["text"]
    s = g["strategy"]
    kws = keywords(crit, e["name"])
    hits = []
    if s == "web_search":
        await out.put(ev("fetch", e=e["id"], c=g["c"], strategy=s, product="SERP API", target=f"{e['name']} {crit}"))
        r = await brightdata.serp(f"{e['name']} {crit}")
        hits.append(r)
        snippet = passages(r["text"], kws)
        url = r["results"][0]["link"] if r["results"] else ""
    elif s == "people_search":
        q = f"{e['name']} {crit}"
        await out.put(ev("fetch", e=e["id"], c=g["c"], strategy=s, product="SERP API", target=q))
        r = await brightdata.serp(q)
        hits.append(r)
        snippet = passages(r["text"], kws + keywords(e["org"])) or r["text"][:600]
        url = r["results"][0]["link"] if r["results"] else ""
    else:
        if not e["url"]:
            await out.put(ev("fetch", e=e["id"], c=g["c"], strategy=s, product="SERP API",
                             target=f"{e['name']} official website", note="resolving domain"))
            r0, site = await resolve_site(e)
            if r0:
                hits.append(r0)
            if site and r0:
                e["url"] = site
                _, ms = await gq("MATCH (e:Entity {id:$id}) SET e.url=$u", {"id": e["id"], "u": site})
                await out.put(ev("graph", op="remember_site", cypher="MATCH (e:Entity {id:$id}) SET e.url=$u",
                                 ms=ms, e=e["id"], url=site))
        if not e["url"]:
            snippet, url = "", ""
        else:
            product = "Web Unlocker" if s == "read_site" else "Browser API"
            await out.put(ev("fetch", e=e["id"], c=g["c"], strategy=s, product=product, target=e["url"]))
            r = await (brightdata.unlocker(e["url"]) if s == "read_site" else brightdata.browser(e["url"]))
            hits.append(r)
            snippet = passages(r["text"], kws)
            url = e["url"]
    if s in ("web_search", "people_search") and hits:
        snippet, url = serp_evidence(hits[-1]["results"], e, kws)
    last = hits[-1] if hits else {"product": "?", "ms": 0, "bytes": 0}
    ok = len(snippet) >= 80
    outcome = f"{last['bytes']}B, {'relevant text found' if ok else 'nothing relevant'}"
    session.tried.setdefault((g["e"], g["c"]), []).append({"strategy": s, "outcome": outcome})
    await out.put(ev("evidence", e=e["id"], c=g["c"], strategy=s, product=last["product"], url=url,
                     ms=sum(h["ms"] for h in hits), bytes=last["bytes"], snippet=snippet, ok=ok,
                     calls=[{"product": h["product"], "target": h["target"], "ms": h["ms"], "bytes": h["bytes"]}
                            for h in hits]))
    return ok, snippet, url, last["product"]


def serp_evidence(results: list[dict], e: dict, kws: list[str]) -> tuple[str, str]:
    name = e["name"].split(",")[0].lower()
    tokens = [t for t in re.findall(r"[a-z0-9]+", name) if len(t) > 2] or [name]
    about = [x for x in results if x["link"].startswith("http") and "google." not in x["link"]
             and any(t in (x["title"] + " " + x["description"] + " " + x["link"]).lower() for t in tokens[-1:])]
    if not about:
        return "", ""
    def rel(x):
        t = (x["title"] + " " + x["description"]).lower()
        return sum(t.count(k) for k in kws)
    about.sort(key=lambda x: -rel(x))
    if rel(about[0]) == 0:
        return "", ""
    text = " … ".join(f"{x['title']}: {x['description']}" for x in about[:3] if rel(x) > 0)
    return text[:700], about[0]["link"]


async def rescore(session: Session, g: dict, snippet: str, url: str, product: str, out: asyncio.Queue):
    e = ENTITIES[g["e"]]
    crit = session.criteria[g["c"]]
    state = f"{e['name']} ({e['tier']}): {e['description']}\n\nEvidence gathered from {url}:\n{snippet}"
    q = {"type": "noul", "instructions": f"Based on the evidence, does {e['name']} satisfy: {crit['text']}?",
         "criteria": {"false": "No, or the evidence does not specifically show it",
                      "true": "Yes, the evidence specifically shows it"}}
    answers, ms = await systemone(config.SCORE_MODEL, state, {"q": q})
    p = float(answers["q"]["noul"])
    before = session.cells[(g["e"], g["c"])]["p"]
    evidence = {"product": product, "url": url, "snippet": snippet}
    session.cells[(g["e"], g["c"])] = {"p": p, "src": "evidence", "evidence": evidence}
    await out.put(ev("cells", cells=[{"e": g["e"], "c": g["c"], "p": round(p, 4), "before": round(before, 4),
                                      "src": "evidence", "evidence": evidence, "ms": ms}]))
    await out.put(ev("rank", rows=session.ranking()))
    cy = ("MATCH (e:Entity {id:$e}), (c:Criterion {id:$c}) MERGE (e)-[s:SCORED]->(c) SET s.p=$p, s.ev=1 "
          "CREATE (x:Evidence {url:$u, product:$prod, snippet:$snip, at:timestamp()}) "
          "CREATE (x)-[:ABOUT]->(e) CREATE (x)-[:INFORMS]->(c)")
    _, gms = await gq(cy, {"e": g["e"], "c": g["c"], "p": p, "u": url, "prod": product, "snip": snippet})
    await out.put(ev("graph", op="evidence", cypher=cy, ms=gms, e=g["e"], c=g["c"]))


async def fill_gap(session: Session, g: dict, out: asyncio.Queue):
    for attempt in range(2):
        if attempt:
            ms = await route(session, [g])
            if not g["strategy"]:
                break
            await out.put(ev("route", e=g["e"], c=g["c"], probs=g["probs"], chosen=g["strategy"], ms=ms,
                             model=config.ROUTER_MODEL, attempt=attempt + 1))
        try:
            ok, snippet, url, product = await execute(session, g, out)
        except Exception as x:
            await out.put(ev("evidence", e=g["e"], c=g["c"], strategy=g["strategy"], ok=False, error=str(x)[:200]))
            session.tried.setdefault((g["e"], g["c"]), []).append({"strategy": g["strategy"], "outcome": "error"})
            continue
        if ok:
            await rescore(session, g, snippet, url, product, out)
            return
    await out.put(ev("unresolved", e=g["e"], c=g["c"]))


async def turn(session: Session, text: str):
    t0 = time.time()
    yield ev("user", text=text)
    before = set(session.criteria)
    scope_before = session.scope
    if config.TRUEFORGE_URL:
        yield ev("agent", phase="start", model=config.TRUEFORGE_MODEL, via="TrueForge")
        async for e in agent_trueforge(session, text):
            yield e
    else:
        yield ev("agent", phase="start", model=config.LLM_MODEL)
        out, ms = await agent(session, text)
        yield ev("agent", phase="done", model=config.LLM_MODEL, ms=ms, ops=out.get("ops", []), reply=out.get("reply", ""))
        for e in await apply_ops(session, out.get("ops", [])):
            yield e
    yield ev("rubric", criteria=list(session.criteria.values()), scope=session.scope)
    yield ev("rank", rows=session.ranking())

    active = list(session.criteria)
    added = [c for c in active if c not in before]
    if session.scope != scope_before and not added and active:
        async for e in score_new(session, active):
            yield e
    if added:
        cells, have, cy, ms = await recall(session, added)
        if cells:
            yield ev("memory", cypher=cy, ms=ms, n=len(cells))
            yield ev("cells", cells=cells)
            yield ev("rank", rows=session.ranking())
        missing = [c for c in added
                   if any((e["id"], c) not in have for e in session.entities())]
        if missing:
            async for e in score_new(session, missing):
                yield e
    elif session.scope == scope_before:
        yield ev("note", text="No new criteria: re-ranked from existing scores, zero model calls.")

    shown = [ENTITIES[r["id"]] for r in session.ranking()[:SHOW_ROWS] if r["score"] > 0]
    holes = [e for e in shown if any((e["id"], c) not in session.cells for c in active)]
    if holes and active:
        yield ev("fill", n=len(holes), names=[e["name"] for e in holes])
        async for e in score_cells(session, active, holes):
            yield e

    ranking = session.ranking()
    gaps = []
    for rank_i, row in enumerate(ranking[:TOP_FOR_GAPS]):
        if row["score"] <= 0:
            continue
        e = ENTITIES[row["id"]]
        if e["kind"] == "talk":
            continue
        thin = e["kind"] == "speaker" or len(e["description"]) < 90
        for cid, c in session.criteria.items():
            cell = session.cells.get((row["id"], cid))
            if not cell or c["weight"] <= 0 or cell.get("evidence") or cell["src"] == "evidence" \
                    or (row["id"], cid) in session.tried or cell["p"] >= GAP_HI:
                continue
            in_band = cell["p"] >= GAP_LO or thin
            if not in_band and rank_i >= 3:
                continue
            value = c["weight"] * (1 - cell["p"]) * (1.0 if in_band else 0.7) / (1 + rank_i * 0.3)
            gaps.append({"e": row["id"], "c": cid, "p": cell["p"], "value": value})
    gaps.sort(key=lambda g: -g["value"])
    gaps = gaps[:MAX_GAPS]
    for g in gaps:
        yield ev("gap", e=g["e"], c=g["c"], p=round(g["p"], 4))
    if gaps:
        ms = await route(session, gaps)
        for g in gaps:
            if g["strategy"]:
                yield ev("route", e=g["e"], c=g["c"], probs=g.get("probs", {}), chosen=g["strategy"], ms=ms,
                         model=config.ROUTER_MODEL, attempt=1)
        q: asyncio.Queue = asyncio.Queue()
        tasks = [asyncio.create_task(fill_gap(session, g, q)) for g in gaps if g["strategy"]]
        done = asyncio.gather(*tasks)
        while not done.done() or not q.empty():
            try:
                yield await asyncio.wait_for(q.get(), timeout=0.25)
            except asyncio.TimeoutError:
                pass
        await done
    yield ev("done", ms=int((time.time() - t0) * 1000))
