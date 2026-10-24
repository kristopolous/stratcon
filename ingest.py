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
    else:
        asyncio.run(main())
