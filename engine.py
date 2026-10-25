"""StratCon engine. A turn is a stream of events, each one a real call that happened."""
import asyncio
import json
import re
import time
from datetime import datetime, timedelta

import httpx
from falkordb import FalkorDB

import brightdata
import cache
import config

graph = None

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


async def gq(cypher: str, params: dict | None = None, timeout: float = 15):
    t = time.time()
    if graph is None:
        for attempt in range(10):
            try:
                await asyncio.to_thread(_reconnect)
                break
            except Exception:
                await asyncio.sleep(3)
    try:
        res = await asyncio.wait_for(asyncio.to_thread(graph.query, cypher, params or {}), timeout)
    except Exception as x:
        if not isinstance(x, asyncio.TimeoutError) and not any(k in str(x).lower() for k in ("pipe", "connection", "closed", "timeout")):
            raise
        await asyncio.to_thread(_reconnect)
        res = await asyncio.wait_for(asyncio.to_thread(graph.query, cypher, params or {}), timeout)
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
                       "e.when, e.room, e.logo, e.speakers, e.photo")
    ENTITIES.clear()
    for i, k, n, t, d, u, o, w, r, lg, sp, ph in rows:
        ENTITIES[i] = {"id": i, "kind": k, "name": n, "tier": t, "description": d, "url": u or "", "org": o or "",
                       "when": w or "", "room": r or "", "logo": ph or lg or "", "speakers": sp or ""}


# Mirrors storyclass.py: every request declares num_ctx (top level, next to keep_alive), stays inside a
# per-model character budget (~3.5 chars/token, question scaffold included) and the 64 KiB payload cap,
# retries 429/5xx with backoff, and on "too large" splits the question batch or halves the state.
MODEL_NUM_CTX = {"tev1": 2048, "clef-flash": 8192, "nimble": 8192, "nimble-v3": 8192, "clef": 16384, "jev": 16384,
                 "systemone": 16384}
MODEL_CHAR_LIMITS = {"tev1": 3500, "clef-flash": 15000, "nimble": 12000, "nimble-v3": 12000, "clef": 20000,
                     "jev": 20000, "systemone": 20000}
DEFAULT_NUM_CTX = 8192
DEFAULT_CHARS = 12000
MAX_PAYLOAD = 60_000
MAX_QUESTIONS = 64


def _base(model: str) -> str:
    return model.split(":")[0]


def payload_fits(model: str, state: str, questions: dict) -> bool:
    q = json.dumps(questions)
    chars = len(state) + len(q)
    ctx = MODEL_NUM_CTX.get(_base(model), DEFAULT_NUM_CTX)
    return (len(questions) <= MAX_QUESTIONS and len((state + q).encode()) + 300 < MAX_PAYLOAD
            and chars <= MODEL_CHAR_LIMITS.get(_base(model), DEFAULT_CHARS) and chars / 3.5 < ctx * 0.9)


def _too_large(msg: str) -> bool:
    m = msg.lower()
    return "too large" in m or "exceed" in m or "413" in m or "tokens) is too" in m or "batch size" in m


async def _post(model: str, state: str, questions: dict) -> dict:
    body = {"model": model, "state": state, "questions": questions, "keep_alive": "15m",
            "num_ctx": MODEL_NUM_CTX.get(_base(model), DEFAULT_NUM_CTX)}
    last = ""
    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(120, connect=20)) as c:
                r = await c.post(config.SYSTEMONE_URL, json=body)
        except (httpx.TransportError, httpx.TimeoutException) as x:
            last = f"{type(x).__name__}: {x}"
            await asyncio.sleep(2 * attempt)
            continue
        if r.status_code == 200:
            return r.json()["answers"]
        last = f"systemone {model} HTTP {r.status_code}: {r.text[:200]}"
        if r.status_code == 413 or _too_large(r.text):
            raise OverflowError(last)
        if r.status_code == 429 or r.status_code >= 500:
            await asyncio.sleep(2 * attempt)
            continue
        raise RuntimeError(last)
    raise RuntimeError(last)


async def systemone(model: str, state: str, questions: dict, _depth: int = 0) -> tuple[dict, int]:
    t = time.time()
    ck = cache.key(model, state, questions)
    if (hit := cache.get("systemone", ck)) is not None:
        return hit, 0
    try:
        if not payload_fits(model, state, questions):
            raise OverflowError("over budget before sending")
        answers = await _post(model, state, questions)
    except OverflowError:
        if _depth >= 4:
            raise
        if len(questions) > 1:
            keys = list(questions)
            half = len(keys) // 2
            a1, _ = await systemone(model, state, {k: questions[k] for k in keys[:half]}, _depth + 1)
            a2, _ = await systemone(model, state, {k: questions[k] for k in keys[half:]}, _depth + 1)
            answers = {**a1, **a2}
        elif len(state) > 800:
            answers, _ = await systemone(model, state[:len(state) // 2] + "\n[... truncated ...]", questions, _depth + 1)
        else:
            raise
    if _depth == 0:
        cache.put("systemone", ck, answers)
    return answers, int((time.time() - t) * 1000)


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
        self.gen = 0
        self.tasks: list[asyncio.Task] = []
        self.calls: dict[str, list] = {}
        self.pins: list[str] = []
        self.last_reply = ""
        self.tf_seeded = False

    def entities(self, everything: bool = False):
        pool = ENTITIES.values() if everything else self._scoped()
        return [e for e in pool if not (self.now and e["kind"] == "talk" and talk_window(e)
                                        and talk_window(e)[0] < self.now)]

    def _scoped(self):
        if self.scope == "sponsors":
            return [e for e in ENTITIES.values() if e["kind"] == "sponsor"]
        if self.scope == "speakers":
            return [e for e in ENTITIES.values() if e["kind"] in ("speaker", "person")]
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
(54 expo-hall sponsor booths, 157 talks and sessions, featured speakers, and 175 presenters you could meet). The rubric is a short list of criteria. Each criterion is a \
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
Rules: Be EAGER to rank: whenever the attendee expresses any goal, interest, preference or constraint, even phrased as a question (\"what about X?\", \"anything on Y?\", \"I want to keep up with Z\"), add or adjust criteria so the board re-ranks. When in doubt, add a criterion. At most 5 criteria in total. Default to ADDING criteria: when the attendee adds detail or another signal ("I mean look for X", "that would help", "also"), add it and keep the existing ones. Only remove a criterion when the attendee says it was a mistake or explicitly drops it ("no, not that", "forget X", "instead of X"). "Which talks/sessions" means scope talks; "which booths/sponsors" means \
scope sponsors; "which speakers" / "who should I meet" / "people" means scope speakers; "everything" means scope all. When the attendee changes their mind, edit existing criteria instead of \
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
                         json={"model": config.LLM_MODEL, "messages": msgs, "stream": False, "temperature": 0})
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
    m = re.search(r"\{.*\}", content, re.S)
    out = json.loads(m.group(0)) if m else {"ops": [], "reply": content.strip()[:200]}
    session.history += [{"role": "user", "content": text}, {"role": "assistant", "content": json.dumps(out)}]
    return out, int((time.time() - t) * 1000)


EAGER = ('Be EAGER to rank: whenever the attendee expresses any goal, interest, preference or constraint, even phrased as a question ("what about X?", "anything on Y?", "I want to keep up with Z"), add or adjust criteria so the board re-ranks. When in doubt, add a criterion. ')
TF_PROMPT = AGENT_PROMPT.split("Given the current rubric")[0] + """""" + EAGER + """Only a pure question about the CURRENT picks (e.g. "what positions are they hiring for?", "who \
is this person?") leaves the rubric alone: call current_recommendations, then research (live web via Bright Data) \
and/or graph_query (read-only FalkorDB Cypher), and answer in 2-3 sentences naming sources. \
Edit the rubric only through the stratcon tools: \
add_criterion, reweight, remove_criterion, set_scope, set_clock (when they say what day/time it is at the \
conference, Oct 28-29 2026), export_calendar (when they want their plan in a calendar); call get_rubric if you \
need criterion ids. Make every edit \
the attendee's message implies, then reply with one short sentence. Default to ADDING criteria: when the attendee adds detail or another signal ("I mean look for X", "that would help", "also"), add it and keep the existing ones. Only remove a criterion when the attendee says it was a mistake or explicitly drops it ("no, not that", "forget X", "instead of X"). Rules: at most 5 criteria. "matters less" / \
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
        spec = t.AgentSpec(model=t.Model(name=config.TRUEFORGE_MODEL, params=t.ModelParams(temperature=0)),
                           instructions=TF_PROMPT,
                           mcp_servers=[t.McpServer(name=config.MCP_NAME, enable_tools=["@all"])])
        r = await c.sessions.create(agent=t.SessionAgentSpecBody(spec=spec), metadata={"stratcon_sid": session.sid})
        session.tf_id = getattr(getattr(r, "data", r), "id")
        yield ev("tf_session", id=session.tf_id, ms=int((time.time() - t0) * 1000))
    session.ops = []
    ACTIVE = session
    rubric = [{"id": k["id"], "text": k["text"], "weight": k["weight"]} for k in session.criteria.values()]
    earlier = ""
    if session.history and not session.tf_seeded:
        earlier = "Earlier in this conversation:\n" + "\n".join(
            f"{h['role']}: {h['content'][:300]}" for h in session.history[-6:]) + "\n\n"
    session.tf_seeded = True
    reply, status = "", ""
    calls: dict[tuple, dict] = {}
    by_id: dict[str, dict] = {}
    text_parts: dict[str, str] = {}
    try:
        res = c.sessions.create_turn_stream(
            session_id=session.tf_id,
            input=[t.UserMessage(type="user.message",
                                 content=f"{earlier}Current rubric: {json.dumps(rubric)}\nScope: {session.scope}\n"
                                         f"Pinned by attendee: {json.dumps([ENTITIES[p]['name'] for p in session.pins])}\n"
                                         f"Attendee says: {text}")])
        if inspect.isawaitable(res):
            res = await res
        async for e in res:
            kind = getattr(e, "type", "")
            if kind == "mcp.initialize":
                yield ev("tf_mcp", servers=[getattr(m, "name", "") for m in (e.mcp_servers or [])])
            elif kind == "model.message.delta":
                if e.content:
                    text_parts[e.id] = text_parts.get(e.id, "") + e.content
                    reply = text_parts[e.id].strip()
                for tc in e.tool_calls or []:
                    c = calls.setdefault((e.id, tc.index), {"id": None, "name": "", "args": "", "server": ""})
                    if tc.id:
                        c["id"] = tc.id
                        by_id[tc.id] = c
                    if tc.function and tc.function.name:
                        c["name"] += tc.function.name
                    if tc.function and tc.function.arguments:
                        c["args"] += tc.function.arguments
                    info = getattr(tc, "tool_info", None)
                    if info is not None and getattr(info, "name", None):
                        c["tool"] = getattr(info, "name", "")
                        c["server"] = getattr(info, "server_name", None) or getattr(info, "type", "")
            elif kind == "model.message":
                if _text(e.content).strip():
                    reply = _text(e.content).strip()
                for tc in e.tool_calls or []:
                    by_id.setdefault(tc.id, {"id": tc.id, "name": tc.function.name, "args": tc.function.arguments or ""})
            elif kind == "tool.response":
                c = by_id.get(e.tool_call_id)
                if c and not c.get("shown"):
                    c["shown"] = True
                    yield ev("tf_tool_call", id=e.tool_call_id, name=c.get("tool") or c["name"], harness_tool=c["name"],
                             args=c["args"], server=c.get("server", ""))
                yield ev("tf_tool_result", id=e.tool_call_id, content=_text(e.content)[:300])
            elif kind == "turn.done":
                status = getattr(e.state, "status", "")
            while not session.inbox.empty():
                yield session.inbox.get_nowait()
    finally:
        if ACTIVE is session:
            ACTIVE = None
    while not session.inbox.empty():
        yield session.inbox.get_nowait()
    session.last_reply = reply
    yield ev("agent", phase="done", model=config.TRUEFORGE_MODEL, via="TrueForge", status=status,
             ms=int((time.time() - t0) * 1000), ops=session.ops, reply=reply)


def resolve_crit(session: "Session", ref) -> str | None:
    """Accept a criterion id or its text; None if it doesn't name a current criterion."""
    if not ref or not isinstance(ref, str):
        return None
    if ref in session.criteria:
        return ref
    want = ref.strip().lower()
    for cid, c in session.criteria.items():
        if c["text"].strip().lower() == want or slug(ref) == cid:
            return cid
    return None


async def gw(events: list, op: str, cy: str, params: dict, **kw):
    try:
        _, ms = await gq(cy, params)
        events.append(ev("graph", op=op, cypher=cy, ms=ms, **kw))
    except Exception as x:
        events.append(ev("graph", op=op, cypher=cy, ms=0, error=str(x)[:160], **kw))


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
            await gw(events, "add", cy, {"sid": session.sid, "cid": cid, "text": op["text"], "w": w}, crit=cid)
        elif kind == "remove" and resolve_crit(session, op.get("id")):
            cid = resolve_crit(session, op.get("id"))
            session.criteria.pop(cid)
            cy = "MATCH (:User {id:$sid})-[r:WANTS]->(:Criterion {id:$cid}) DELETE r"
            await gw(events, "remove", cy, {"sid": session.sid, "cid": cid}, crit=cid)
        elif kind == "reweight" and resolve_crit(session, op.get("id")):
            cid = resolve_crit(session, op.get("id"))
            w = max(-1.0, min(1.0, float(op.get("weight", 0.5))))
            events.append(ev("reweight", crit=cid, before=session.criteria[cid]["weight"], after=w))
            session.criteria[cid]["weight"] = w
            cy = "MATCH (:User {id:$sid})-[r:WANTS]->(:Criterion {id:$cid}) SET r.weight=$w"
            await gw(events, "reweight", cy, {"sid": session.sid, "cid": cid, "w": w}, crit=cid)
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
            events.append(ev("calendar", url=f"api/calendar.ics?sid={session.sid}",
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
    allowed = {e["id"] for e in ents}
    rel: dict[str, float] = {e["id"]: 0.0 for e in ents}
    keep: set[str] = set()
    from ingest import TOPICS as _T, ROLES as _R
    TOPICS = {**_T, **_R}
    for cid in crits:
        text = session.criteria[cid]["text"]
        a, ms = await systemone(config.ROUTER_MODEL, "Map an attendee's criterion onto conference topics.",
                                {"t": {"type": "choice", "instructions": f"Which topic does this criterion belong to: {text}?",
                                       "criteria": TOPICS}})
        probs = sorted(a["t"]["probabilities"].items(), key=lambda x: -x[1])
        topics = [{"t": t, "p": round(p, 3)} for t, p in probs[:3] if p >= 0.1 or t == probs[0][0]]
        cy = ("MATCH (c:Criterion {id:$cid}) OPTIONAL MATCH (c)-[m:MAPS_TO]->() DELETE m WITH DISTINCT c "
              "UNWIND $topics AS x MATCH (t:Topic {id:x.t}) CREATE (c)-[:MAPS_TO {p:x.p}]->(t)")
        _, gms = await gq(cy, {"cid": cid, "topics": topics})
        yield ev("topics", crit=cid, model=config.ROUTER_MODEL, ms=ms, topics=[[TOPICS[x["t"]], x["p"]] for x in topics],
                 cypher=cy, gms=gms)
        cy = ("MATCH (c:Criterion {id:$cid}), (e:Entity) "
              "WITH c, e, shortestPath((c)-[:MAPS_TO|COVERS|PRESENTED_BY|WORKS_AT|EMPLOYS|PRESENTS*..5]->(e)) AS p "
              "WHERE p IS NOT NULL "
              "RETURN e.id, length(p), reduce(w = 1.0, r IN relationships(p) | w * coalesce(r.p, 0.6)) AS w "
              "ORDER BY length(p), w DESC LIMIT 60")
        rows, ms = await gq(cy, {"cid": cid})
        rows = [r for r in rows if r[0] in allowed]
        for eid, hops, w in rows:
            rel[eid] += float(w) / hops
        direct = [r for r in rows if r[1] <= 3][:RETRIEVE_PER_CRIT + 3]
        hops2 = [r for r in rows if r[1] > 3][:3]
        keep |= {r[0] for r in direct} | {r[0] for r in hops2}
        yield ev("graph_search", crit=cid, cypher=cy, ms=ms, hits=len(rows),
                 direct=[ENTITIES[r[0]]["name"] for r in direct], multihop=[ENTITIES[r[0]]["name"] for r in hops2])
        q = " | ".join(keywords(text)) or text
        cy = "CALL db.idx.fulltext.queryNodes('Entity', $q) YIELD node, score RETURN node.id, score ORDER BY score DESC LIMIT 5"
        rows, ms = await gq(cy, {"q": q.replace("-", " ")})
        rows = [r for r in rows if r[0] in allowed]
        for eid, sc in rows:
            rel[eid] += 0.2 * float(sc)
        keep |= {r[0] for r in rows[:3]}
        yield ev("fulltext", crit=cid, cypher=cy, query=q, ms=ms, hits=[ENTITIES[r[0]]["name"] for r in rows[:3]])
        if len(keep) < 5:
            fstate = "GitHub Universe 2026 sponsor booths, talks and people."
            finstr = f"Which one best satisfies: {text}?"
            pool = sorted(ents, key=lambda e: e["kind"] == "talk")
            opts = {}
            for e in pool:
                trial = {**opts, e["id"]: f"{e['name']} ({e['kind']}{', ' + e['org'] if e.get('org') else ''})"}
                if not payload_fits(config.ROUTER_MODEL, fstate + " " * 200,
                                    {"pick": {"type": "choice", "instructions": finstr, "criteria": trial}}):
                    break
                opts = trial
            a, ms = await systemone(config.ROUTER_MODEL, fstate,
                                    {"pick": {"type": "choice", "instructions": finstr, "criteria": opts}})
            probs = sorted(a["pick"]["probabilities"].items(), key=lambda x: -x[1])
            for i, pr in probs:
                rel[i] += pr
            keep |= {i for i, _ in probs[:RETRIEVE_PER_CRIT]}
            for kind in ("sponsor", "talk", "speaker", "person"):
                keep |= {i for i, _ in [x for x in probs if ENTITIES[x[0]]["kind"] == kind][:2]}
            yield ev("fallback", crit=cid, model=config.ROUTER_MODEL, ms=ms, n=len(opts),
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
          "WITH e, c, s, x ORDER BY x.at DESC "
          "WITH e, c, s, collect(x)[0] AS x "
          "RETURN e.id, c.id, s.p, s.ev, x.product, x.url, x.snippet")
    rows, ms = await gq(cy, {"ids": crits})
    cells, have = [], set()
    for eid, cid, p, evn, prod, url, snip in rows:
        if eid not in ENTITIES or (eid, cid) in have:
            continue
        cell = {"p": float(p), "src": "memory"}
        if prod:
            cell["evidence"] = {"product": prod, "url": url, "snippet": snip}
        session.cells[(eid, cid)] = cell
        have.add((eid, cid))
        cells.append({"e": eid, "c": cid, "p": round(float(p), 4), "src": "memory", "evidence": cell.get("evidence")})
    return cells, have, cy, ms


def site_matches(e: dict, page: str) -> bool:
    """False only when a substantial page shares nothing with the sponsor's own description (clearly another
    company). Short, empty or JavaScript-only pages are inconclusive, so the remembered site is kept."""
    words = [w for w in keywords(e["description"]) if len(w) > 4 and w not in keywords(e["name"])][:25]
    text = page.lower()
    if len(text) < 2000 or len(words) < 4:
        return True
    return sum(1 for w in set(words) if w in text) >= 1


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
        if e["kind"] in ("speaker", "person"):
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
            if not site_matches(e, r.get("text", "")):
                bad = e["url"]
                e["url"] = ""
                await gq("MATCH (e:Entity {id:$id}) SET e.url = ''", {"id": e["id"]})
                await out.put(ev("graph", op="forget_site", cypher="MATCH (e:Entity {id:$id}) SET e.url = ''", ms=0,
                                 e=e["id"], url=bad))
                snippet, url = "", bad
            else:
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
                     cached=all(h.get("cached") for h in hits) if hits else False,
                     calls=[{"product": h["product"], "target": h["target"], "ms": h["ms"], "bytes": h["bytes"],
                             "cached": bool(h.get("cached"))} for h in hits]))
    calls = [{"product": h["product"], "strategy": s, "target": h["target"], "ms": h["ms"], "bytes": h["bytes"],
              "cached": bool(h.get("cached"))} for h in hits]
    session.calls.setdefault(e["id"], []).extend([{**c, "ok": ok, "criterion": crit} for c in calls])
    return ok, snippet, url, last["product"], calls


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


async def rescore(session: Session, g: dict, snippet: str, url: str, product: str, out: asyncio.Queue,
                  calls: list | None = None):
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
          "CREATE (x:Evidence {url:$u, product:$prod, snippet:$snip, calls:$calls, at:timestamp()}) "
          "CREATE (x)-[:ABOUT]->(e) CREATE (x)-[:INFORMS]->(c)")
    _, gms = await gq(cy, {"e": g["e"], "c": g["c"], "p": p, "u": url, "prod": product, "snip": snippet,
                           "calls": json.dumps(calls or [])})
    await out.put(ev("graph", op="evidence", cypher=cy, ms=gms, e=g["e"], c=g["c"]))


async def fill_gap(session: Session, g: dict, out: asyncio.Queue):
    # Keep retrying with a different Bright Data strategy until one yields evidence or all have been tried.
    for attempt in range(len(STRATEGIES)):
        if attempt:
            ms = await route(session, [g])
            if not g["strategy"]:
                break
            await out.put(ev("route", e=g["e"], c=g["c"], probs=g["probs"], chosen=g["strategy"], ms=ms,
                             model=config.ROUTER_MODEL, attempt=attempt + 1))
        try:
            ok, snippet, url, product, calls = await execute(session, g, out)
        except Exception as x:
            await out.put(ev("evidence", e=g["e"], c=g["c"], strategy=g["strategy"], ok=False, error=str(x)[:200]))
            session.tried.setdefault((g["e"], g["c"]), []).append({"strategy": g["strategy"], "outcome": "error"})
            continue
        if ok:
            await rescore(session, g, snippet, url, product, out, calls)
            return
    await out.put(ev("unresolved", e=g["e"], c=g["c"], tries=len(session.tried.get((g["e"], g["c"]), []))))


WHY_PATH = ("MATCH (u:User {id:$sid}), (e:Entity {id:$id}) "
            "WITH u, e, shortestPath((u)-[:WANTS|MAPS_TO|COVERS|PRESENTED_BY|WORKS_AT|EMPLOYS|PRESENTS*..7]->(e)) AS p "
            "WHERE p IS NOT NULL "
            "RETURN [n IN nodes(p) | [labels(n)[0], coalesce(n.id, ''), coalesce(n.name, n.text, '')]], "
            "[r IN relationships(p) | type(r)]")


async def explain(session: Session, n: int = 3):
    """Each top recommendation's reason: the shortest path from the attendee to it, plus the evidence on that edge."""
    items, total = [], 0
    for r in [r for r in session.ranking() if r["score"] > 0][:n]:
        e = ENTITIES[r["id"]]
        rows, ms = await gq(WHY_PATH, {"sid": session.sid, "id": e["id"]})
        total += ms
        if not rows:
            continue
        nodes, rels = rows[0]
        best = None
        for cid, c in session.criteria.items():
            cell = session.cells.get((e["id"], cid))
            if cell and c["weight"] > 0 and (best is None or c["weight"] * cell["p"] > best[0]):
                best = (c["weight"] * cell["p"], c["text"], cell)
        items.append({"id": e["id"], "nodes": nodes, "rels": rels,
                      "p": round(best[2]["p"], 2) if best else None, "criterion": best[1] if best else "",
                      "evidence": (best[2].get("evidence") or {}) if best else {}})
    return items, [WHY_PATH], total


async def turn(session: Session, text: str):
    t0 = time.time()
    yield ev("user", text=text)
    try:
        await gq("MERGE (u:User {id:$sid}) ON CREATE SET u.title = $text, u.started = timestamp() "
                 "SET u.updated = timestamp(), u.pins = $pins", {"sid": session.sid, "text": text[:120], "pins": session.pins})
    except Exception:
        pass
    before = set(session.criteria)
    scope_before = session.scope
    if config.TRUEFORGE_URL:
        yield ev("agent", phase="start", model=config.TRUEFORGE_MODEL, via="TrueForge")
        async for e in agent_trueforge(session, text):
            yield e
    else:
        yield ev("agent", phase="start", model=config.LLM_MODEL)
        out, ms = await agent(session, text)
        session.last_reply = out.get("reply", "")
        yield ev("agent", phase="done", model=config.LLM_MODEL, ms=ms, ops=out.get("ops", []), reply=out.get("reply", ""))
        for e in await apply_ops(session, out.get("ops", [])):
            yield e
    yield ev("rubric", criteria=list(session.criteria.values()), scope=session.scope)
    yield ev("rank", rows=session.ranking())

    try:
        await gq("MATCH (u:User {id:$sid}) SET u.scope = $scope, u.now = $now "
                 "CREATE (:Turn {text:$text, reply:$reply, at:timestamp()})-[:IN]->(u)",
                 {"sid": session.sid, "scope": session.scope, "text": text, "reply": session.last_reply,
                  "now": session.now.isoformat() if session.now else ""})
    except Exception:
        pass
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
        thin = e["kind"] in ("speaker", "person") or len(e["description"]) < 90
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
        session.tasks.extend(tasks)
        done = asyncio.gather(*tasks)
        while not done.done() or not q.empty():
            try:
                yield await asyncio.wait_for(q.get(), timeout=0.25)
            except asyncio.TimeoutError:
                pass
        await done
    items, cyphers, ms = await explain(session)
    if items:
        yield ev("why", items=items, cypher=cyphers, ms=ms)
    yield ev("done", ms=int((time.time() - t0) * 1000))
