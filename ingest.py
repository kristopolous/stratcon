"""Pull the real GitHub Universe 2026 sponsors, speakers and sessions into FalkorDB."""
import asyncio
import re
import sys
import time

from falkordb import FalkorDB

import brightdata
import config

PORTAL = "https://reg.githubuniverse.com/flow/github/universe26/attendee-portal/page"
SPONSORS_URL = f"{PORTAL}/sponsors"
SESSIONS_URL = f"{PORTAL}/sessioncatalog"
HOME_URL = "https://githubuniverse.com/"
TIERS = {"Platinum", "Gold", "Silver", "Bronze", "Signal"}
TAGS = {"In-person", "Recorded", "Livestream", "AI summary", "Virtual", "On-demand"}
TYPES = {"Social", "Special Event", "Keynote", "Breakout Session", "Product Demo", "Ship & Tell", "Sandbox Session",
         "Session After Party", "Workshop", "Main Stage", "Lightning Talk", "Panel", "Theater Session", "Fireside Chat", "Panel Discussion"}
END = {"Show more", "Show less"}
DAY = re.compile(r"^(Mon|Tues|Wednes|Thurs|Fri|Satur|Sun)day, Oct \d+$")
TIME = re.compile(r"^\d{1,2}:\d{2} [AP]M - \d{1,2}:\d{2} [AP]M")


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:60]


def parse_sponsors(text: str, links: list[str], images: list[list[str]]) -> list[dict]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    start = lines.index("Clear") + 1
    out, tier, block = [], None, []
    for l in lines[start:]:
        if l.startswith("©"):
            break
        if l in TIERS:
            tier = l
        elif l == "Visit sponsor":
            if block:
                out.append({"name": block[0], "description": " ".join(block[1:]), "tier": tier})
            block = []
        else:
            block.append(l)
    logos = {alt[:-len(" banner")]: src for alt, src in images if alt.endswith(" banner")}
    for s, link in zip(out, links):
        s["url"] = link if link.startswith("http") else ""
    for s in out:
        s["logo"] = logos.get(s["name"], "")
    return out


def parse_speakers(md: str) -> list[dict]:
    avatars = {n: p for n, p in re.findall(r"!\[([^\]]+)'s avatar\]\(([^)]+)\)", md)}
    return [{"name": n.strip(), "org": o.strip(), "logo": HOME_URL.rstrip("/") + avatars.get(n.strip(), "") if avatars.get(n.strip()) else ""}
            for n, o in re.findall(r"avatar\]\([^)]*\)([^,!\]]+), ([^!]+?)\1, \2", md)]


def parse_sessions(text: str) -> list[dict]:
    lines = [l.strip() for l in text.splitlines() if l.strip() and l.strip() not in ("Add to agenda", "Remove from agenda")]
    try:
        start = next(i for i, l in enumerate(lines) if re.match(r"^Thu, Oct 29 \(\d+\)$", l)) + 1
    except StopIteration:
        start = 0
    out, i, block = [], start, []
    while i < len(lines):
        l = lines[i]
        if l in ("Show more", "Show less") or l.startswith("©"):
            break
        if l == "See details" and i + 2 < len(lines) and DAY.match(lines[i + 1]) and TIME.match(lines[i + 2]):
            day, when = lines[i + 1], lines[i + 2]
            room, nxt = "", i + 3
            if nxt < len(lines) and lines[nxt] not in END and not lines[nxt].startswith("©") and \
                    (nxt + 1 >= len(lines) or lines[nxt + 1] not in TYPES):
                room, nxt = lines[nxt], nxt + 1
            if block:
                title, kind = block[0], block[1] if len(block) > 1 else ""
                speakers, abstract = [], []
                for j, b in enumerate(block[2:], start=2):
                    if b.startswith("/") and j > 2:
                        parts = [p for p in b.split("/") if p]
                        speakers.append({"name": block[j - 1], "title": parts[0] if parts else "", "org": parts[-1] if parts else ""})
                for j, b in enumerate(block[2:], start=2):
                    if b in TAGS or b.startswith("/") or b.startswith(", ") or (j + 1 < len(block) and block[j + 1].startswith("/")):
                        continue
                    abstract.append(b)
                out.append({"title": title, "type": kind, "abstract": " ".join(abstract), "speakers": speakers,
                            "day": day, "time": when.replace(" PDT", ""), "room": room})
            block, i = [], nxt
            continue
        block.append(l)
        i += 1
    return out


async def main():
    t = time.time()
    spon, sess, home = await asyncio.gather(
        brightdata.browser(SPONSORS_URL, scroll=6, links_text="Visit sponsor", wait_text="Visit sponsor"),
        brightdata.browser(SESSIONS_URL, scroll=2, click_text="Show more", clicks=5, wait_text="See details"),
        brightdata.unlocker(HOME_URL),
    )
    sponsors = parse_sponsors(spon["text"], spon["links"], spon["images"])
    sessions = parse_sessions(sess["text"])
    speakers = parse_speakers(home["text"])
    print(f"Browser API: {len(sponsors)} sponsors, {sum(1 for s in sponsors if s['logo'])} logos ({spon['ms']}ms)")
    print(f"Browser API: {len(sessions)} sessions ({sess['ms']}ms)")
    print(f"Web Unlocker: {len(speakers)} featured speakers ({home['ms']}ms)")

    g = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)
    try:
        g.delete()
    except Exception:
        pass
    g.query("CREATE INDEX FOR (e:Entity) ON (e.id)")
    g.query("CREATE INDEX FOR (c:Criterion) ON (c.id)")
    g.query("CREATE INDEX FOR (p:Person) ON (p.id)")
    for page in (spon, sess, home):
        g.query("CREATE (:Source {url:$u, product:$p, ms:$ms, bytes:$b, at:timestamp()})",
                {"u": page["target"], "p": page["product"], "ms": page["ms"], "b": page["bytes"]})
    for s in sponsors:
        g.query("""MATCH (src:Source {url:$src})
                   CREATE (:Entity {id:$id, kind:'sponsor', name:$name, tier:$tier, description:$d, url:$url, logo:$logo})
                          -[:LISTED_ON]->(src)""",
                {"src": SPONSORS_URL, "id": slug(s["name"]), "name": s["name"], "tier": s["tier"] or "",
                 "d": s["description"], "url": s["url"], "logo": s["logo"]})
    for p in speakers:
        g.query("""MATCH (src:Source {url:$src})
                   CREATE (:Entity {id:$id, kind:'speaker', name:$name, org:$org, tier:'Featured speaker',
                                    description:$d, url:'', logo:$logo})-[:LISTED_ON]->(src)""",
                {"src": HOME_URL, "id": slug(p["name"]), "name": p["name"], "org": p["org"], "logo": p["logo"],
                 "d": f"{p['name']} ({p['org']}) is a featured speaker at GitHub Universe 2026."})
    for s in sessions:
        who = ", ".join(f"{p['name']} ({p['org']})" for p in s["speakers"])
        tid = "talk-" + slug(s["title"])
        g.query("""MATCH (src:Source {url:$src})
                   MERGE (t:Entity {id:$id})
                   SET t.kind='talk', t.name=$name, t.tier=$type, t.description=$d, t.url='', t.logo='',
                       t.when=$when, t.room=$room, t.speakers=$who
                   MERGE (t)-[:LISTED_ON]->(src)""",
                {"src": SESSIONS_URL, "id": tid, "name": s["title"], "type": s["type"], "room": s["room"],
                 "when": f"{s['day']} · {s['time']}", "who": who,
                 "d": (s["abstract"] + (f" Speakers: {who}." if who else ""))[:900]})
        for p in s["speakers"]:
            g.query("""MATCH (t:Entity {id:$tid})
                       MERGE (p:Person {id:$pid}) SET p.name=$name, p.org=$org, p.title=$title
                       MERGE (p)-[:PRESENTS]->(t)
                       WITH p OPTIONAL MATCH (e:Entity {kind:'speaker', id:$pid})
                       FOREACH (_ IN CASE WHEN e IS NULL THEN [] ELSE [1] END | MERGE (e)-[:PRESENTS]->(t))""",
                    {"tid": tid, "pid": slug(p["name"]), "name": p["name"], "org": p["org"], "title": p["title"]})
    n = g.query("MATCH (e:Entity) RETURN e.kind, count(e)").result_set
    links = g.query("MATCH (:Entity {kind:'speaker'})-[:PRESENTS]->(t) RETURN count(t)").result_set[0][0]
    print("FalkorDB:", n, f"featured speakers linked to {links} talks", f"total {time.time()-t:.1f}s")
    for s in sessions[:3]:
        print("  e.g.", s["day"], s["time"], "|", s["title"], "|", s["room"], "|", [p["name"] for p in s["speakers"]][:3])


TOPICS = {
    "ci-cd": "CI/CD pipelines, builds and deployment",
    "testing": "Software testing, QA and test automation",
    "security": "Application security and vulnerability scanning",
    "supply-chain": "Software supply chain, dependencies, SBOMs and artifacts",
    "secrets-identity": "Secrets, identity and access management",
    "observability": "Observability, monitoring, logging and incident response",
    "debugging": "Debugging, error tracking and code comprehension",
    "code-review": "Code review, code quality and static analysis",
    "agents": "AI coding agents and agentic workflows",
    "copilot": "GitHub Copilot features and usage",
    "sandbox": "Sandboxes, isolation and safe execution environments",
    "platform": "Platform engineering, developer portals and internal tooling",
    "ai-infra": "AI models, inference and ML infrastructure",
    "data": "Data platforms, analytics and databases",
    "docs": "Documentation and knowledge management",
    "modernization": "Legacy modernization, migration and consulting services",
    "productivity": "Developer productivity, metrics and engineering management",
    "open-source": "Open source community and maintainers",
    "hardware": "Hardware, devices and IoT",
    "community": "Social events, networking and community",
}


ROLES = {
    "role-academic": "Academics, professors and researchers",
    "role-author": "Authors, educators, teachers and content creators",
    "role-advocate": "Developer advocates and community builders",
    "role-leader": "Engineering leaders and executives (CTO, VP, director)",
    "role-founder": "Founders and startup builders",
    "role-recruiter": "Recruiters, hiring managers and people who hire",
    "role-engineer": "Hands-on software engineers and architects",
    "role-product": "Product managers and designers",
}
TOPICS_ALL = None


async def build_roles():
    """Classify every person by role into Topic nodes, so people-shaped criteria have a graph route."""
    import engine
    g = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)
    for tid, label in ROLES.items():
        g.query("MERGE (t:Topic {id:$id}) SET t.name = $name, t.role = true", {"id": tid, "name": label})
    g.query("MATCH (:Entity)-[a:ABOUT]->(t:Topic) WHERE t.role = true DELETE a")
    people = g.query("""MATCH (p:Entity) WHERE p.kind IN ['person', 'speaker']
                        RETURN p.id, p.name, coalesce(p.title, p.tier, ''), coalesce(p.org, ''), p.description""").result_set
    t0, edges = time.time(), 0
    for start in range(0, len(people), 6):
        chunk = people[start:start + 6]
        qs = {str(i): {"type": "choice", "instructions": f"Which role best describes [{i}] {n}?", "criteria": ROLES}
              for i, (_, n, _, _, _) in enumerate(chunk)}
        state = "\n".join(f"[{i}] {n}, {ti} at {o}. {(d or '')[:200]}" for i, (_, n, ti, o, d) in enumerate(chunk))
        answers, _ = await engine.systemone(config.SCORE_MODEL, state, qs)
        rows = []
        for i, (pid, *_rest) in enumerate(chunk):
            probs = sorted(answers[str(i)]["probabilities"].items(), key=lambda x: -x[1])
            rows += [{"e": pid, "t": t, "p": round(p, 3)} for t, p in probs[:2] if p >= 0.2 or t == probs[0][0]]
        g.query("UNWIND $rows AS r MATCH (e:Entity {id:r.e}), (t:Topic {id:r.t}) CREATE (e)-[:ABOUT {p:r.p}]->(t)", {"rows": rows})
        edges += len(rows)
        print(f"  roles {start + len(chunk)}/{len(people)} ({time.time() - t0:.0f}s)", flush=True)
    print(f"role edges: {edges}")
    for row in g.query("MATCH (t:Topic {role:true})<-[:ABOUT]-(e) RETURN t.id, count(e) ORDER BY count(e) DESC").result_set:
        print("  ", row)
    link_routes()


def link_employers(g) -> int:
    """WORKS_AT by whole-word organisation match, so "Port" doesn't claim people at "Support" or "Passport"."""
    import re
    g.query("MATCH (:Person)-[w:WORKS_AT]->() DELETE w")
    people = g.query("MATCH (p:Person) WHERE p.org IS NOT NULL AND p.org <> '' RETURN p.id, p.org").result_set
    sponsors = g.query("MATCH (s:Entity {kind:'sponsor'}) RETURN s.id, s.name").result_set
    pats = [(sid, re.compile(r"(?<![a-z0-9])" + re.escape(name.split(",")[0].strip().lower()) + r"(?![a-z0-9])"))
            for sid, name in sponsors if name.split(",")[0].strip()]
    rows = [{"p": pid, "s": sid} for pid, org in people for sid, pat in pats if pat.search(org.lower())]
    g.query("UNWIND $rows AS r MATCH (p:Person {id:r.p}), (s:Entity {id:r.s}) CREATE (p)-[:WORKS_AT]->(s)", {"rows": rows})
    return len(rows)


async def build_topics():
    """Classify every entity into topics (System One choice), then link people to the sponsors they work for."""
    import engine
    g = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)
    g.query("MATCH (t:Topic) DETACH DELETE t")
    for tid, label in TOPICS.items():
        g.query("CREATE (:Topic {id:$id, name:$name})", {"id": tid, "name": label})
    try:
        g.query("CALL db.idx.fulltext.createNodeIndex('Topic', 'name')")
    except Exception:
        pass
    try:
        g.query("CALL db.idx.fulltext.createNodeIndex('Entity', 'name', 'description')")
    except Exception:
        pass
    ents = [r for r in g.query("MATCH (e:Entity) RETURN e.id, e.name, e.description").result_set]
    t0, edges = time.time(), 0
    for start in range(0, len(ents), 6):
        chunk = ents[start:start + 6]
        qs = {str(i): {"type": "choice", "instructions": f"Which topic best describes [{i}] {n}?", "criteria": TOPICS}
              for i, (_, n, _) in enumerate(chunk)}
        state = "GitHub Universe 2026:\n" + "\n".join(f"[{i}] {n}: {(d or '')[:300]}" for i, (_, n, d) in enumerate(chunk))
        answers, _ = await engine.systemone(config.ROUTER_MODEL, state, qs)
        rows = []
        for i, (eid, _, _) in enumerate(chunk):
            probs = sorted(answers[str(i)]["probabilities"].items(), key=lambda x: -x[1])
            rows += [{"e": eid, "t": t, "p": round(p, 3)} for t, p in probs[:3] if p >= 0.12 or t == probs[0][0]]
        g.query("UNWIND $rows AS r MATCH (e:Entity {id:r.e}), (t:Topic {id:r.t}) CREATE (e)-[:ABOUT {p:r.p}]->(t)",
                {"rows": rows})
        edges += len(rows)
        print(f"  topics {start + len(chunk)}/{len(ents)} ({time.time() - t0:.0f}s)", flush=True)
    n = link_employers(g)
    print(f"ABOUT edges: {edges}; WORKS_AT edges: {n}; {time.time() - t0:.0f}s")
    for row in g.query("MATCH (t:Topic)<-[a:ABOUT]-(e) RETURN t.id, count(e) ORDER BY count(e) DESC").result_set:
        print("  ", row)
    link_routes()


def link_routes():
    """Directed edges from the attendee outward, so shortestPath can walk You -> ... -> recommendation."""
    g = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)
    for rel in ("COVERS", "PRESENTED_BY", "EMPLOYS"):
        g.query(f"MATCH ()-[r:{rel}]->() DELETE r")
    out = {}
    out["COVERS"] = g.query("MATCH (e:Entity)-[a:ABOUT]->(t:Topic) CREATE (t)-[:COVERS {p:a.p}]->(e) RETURN count(*)").result_set[0][0]
    out["PRESENTED_BY"] = g.query("MATCH (p:Person)-[:PRESENTS]->(t:Entity) CREATE (t)-[:PRESENTED_BY]->(p) RETURN count(*)").result_set[0][0]
    out["EMPLOYS"] = g.query("MATCH (p:Person)-[:WORKS_AT]->(s:Entity) CREATE (s)-[:EMPLOYS]->(p) RETURN count(*)").result_set[0][0]
    print("route edges:", out)


def promote_people():
    """Make every presenter a recommendable person: they inherit topics from the talks they present."""
    g = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)
    n = g.query("""MATCH (p:Person)-[:PRESENTS]->(t:Entity {kind:'talk'})
                   OPTIONAL MATCH (s:Entity {kind:'speaker'}) WHERE s.id = p.id
                   WITH p, s, collect(t.name)[0..2] AS talks WHERE s IS NULL
                   SET p:Entity, p.kind = 'person', p.tier = coalesce(p.title, ''), p.url = '', p.logo = '',
                       p.description = coalesce(p.title, '') + ' at ' + coalesce(p.org, '') +
                                       '. Presenting at GitHub Universe 2026: ' + reduce(acc = '', x IN talks | acc + x + '; ')
                   RETURN count(p)""").result_set[0][0]
    g.query("MATCH (p:Entity {kind:'person'})-[a:ABOUT]->() DELETE a")
    e = g.query("""MATCH (p:Entity {kind:'person'})-[:PRESENTS]->(:Entity {kind:'talk'})-[a:ABOUT]->(t:Topic)
                   WITH p, t, max(a.p) AS w
                   CREATE (p)-[:ABOUT {p: w * 0.8}]->(t) RETURN count(*)""").result_set[0][0]
    print(f"promoted {n} presenters to recommendable people; {e} inherited topic edges")


def reset_memory():
    g = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)
    for label in ("User", "Criterion", "Evidence"):
        n = g.query(f"MATCH (x:{label}) DETACH DELETE x RETURN count(x)").result_set[0][0]
        print(f"deleted {n} {label}")
    n = g.query("MATCH (e:Entity) WHERE e.kind = 'sponsor' AND e.url <> '' SET e.url = '' RETURN count(e)").result_set[0][0]
    print(f"cleared {n} discovered sites")


if __name__ == "__main__":
    if "--reset-memory" in sys.argv:
        reset_memory()
    elif "--topics" in sys.argv:
        asyncio.run(build_topics())
    elif "--links" in sys.argv:
        link_routes()
    elif "--roles" in sys.argv:
        asyncio.run(build_roles())
    elif "--people" in sys.argv:
        promote_people()
        link_routes()
    elif "--employers" in sys.argv:
        print("WORKS_AT edges:", link_employers(FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)))
        link_routes()
    else:
        # The full pipeline, in dependency order: entities, people, topics, roles, then the route edges search walks.
        asyncio.run(main())
        promote_people()
        asyncio.run(build_topics())
        asyncio.run(build_roles())
        link_routes()
