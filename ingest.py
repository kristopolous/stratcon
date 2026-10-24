"""Pull the real GitHub Universe 2026 sponsors and speakers into FalkorDB."""
import asyncio
import re
import time

from falkordb import FalkorDB

import brightdata
import config

SPONSORS_URL = "https://reg.githubuniverse.com/flow/github/universe26/attendee-portal/page/sponsors"
HOME_URL = "https://githubuniverse.com/"
TIERS = {"Platinum", "Gold", "Silver", "Bronze", "Signal"}


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def parse_sponsors(text: str, links: list[str]) -> list[dict]:
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
    for s, link in zip(out, links):
        s["url"] = link if link.startswith("http") else ""
    return out


def parse_speakers(md: str) -> list[dict]:
    return [{"name": n.strip(), "org": o.strip()}
            for n, o in re.findall(r"avatar\]\([^)]*\)([^,!\]]+), ([^!]+?)\1, \2", md)]


async def main():
    t = time.time()
    spon_page, home_page = await asyncio.gather(
        brightdata.browser(SPONSORS_URL, scroll=6, links_text="Visit sponsor"),
        brightdata.unlocker(HOME_URL),
    )
    sponsors = parse_sponsors(spon_page["text"], spon_page["links"])
    speakers = parse_speakers(home_page["text"])
    print(f"Browser API: {len(sponsors)} sponsors ({spon_page['ms']}ms, {spon_page['bytes']}B)")
    print(f"Web Unlocker: {len(speakers)} speakers ({home_page['ms']}ms, {home_page['bytes']}B)")

    g = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)
    try:
        g.delete()
    except Exception:
        pass
    g.query("CREATE INDEX FOR (e:Entity) ON (e.id)")
    g.query("CREATE INDEX FOR (c:Criterion) ON (c.id)")
    g.query("CALL db.idx.fulltext.createNodeIndex('Entity', 'name', 'description')")
    for page in (spon_page, home_page):
        g.query("CREATE (:Source {url:$u, product:$p, ms:$ms, bytes:$b, at:timestamp()})",
                {"u": page["target"], "p": page["product"], "ms": page["ms"], "b": page["bytes"]})
    for s in sponsors:
        g.query("""MATCH (src:Source {url:$src})
                   CREATE (e:Entity {id:$id, kind:'sponsor', name:$name, tier:$tier, description:$d, url:$url})-[:LISTED_ON]->(src)""",
                {"src": SPONSORS_URL, "id": slug(s["name"]), "name": s["name"], "tier": s["tier"] or "",
                 "d": s["description"], "url": s.get("url", "")})
    for p in speakers:
        g.query("""MATCH (src:Source {url:$src})
                   CREATE (e:Entity {id:$id, kind:'speaker', name:$name, org:$org, tier:'Featured speaker',
                                     description:$d, url:''})-[:LISTED_ON]->(src)""",
                {"src": HOME_URL, "id": slug(p["name"]), "name": p["name"], "org": p["org"],
                 "d": f"{p['name']} ({p['org']}) is a featured speaker at GitHub Universe 2026."})
    n = g.query("MATCH (e:Entity) RETURN e.kind, count(e)").result_set
    print("FalkorDB:", n, f"total {time.time()-t:.1f}s")
    for s in sponsors[:3]:
        print("  e.g.", s["tier"], "|", s["name"], "|", s.get("url"))
    print("  speakers:", ", ".join(f"{p['name']} ({p['org']})" for p in speakers))


def reset_memory():
    g = FalkorDB.from_url(config.FALKORDB_URL).select_graph(config.GRAPH)
    for label in ("User", "Criterion", "Evidence"):
        n = g.query(f"MATCH (x:{label}) DETACH DELETE x RETURN count(x)").result_set[0][0]
        print(f"deleted {n} {label}")
    n = g.query("MATCH (e:Entity) WHERE e.url <> '' SET e.url = '' RETURN count(e)").result_set[0][0]
    print(f"cleared {n} discovered sites")


if __name__ == "__main__":
    import sys
    if "--reset-memory" in sys.argv:
        reset_memory()
    else:
        asyncio.run(main())
