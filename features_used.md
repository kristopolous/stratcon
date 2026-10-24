# Sponsor features used in StratCon

StratCon is a recommender you steer while it works. Each sponsor product does a specific job in that loop, and each one leaves visible traces on screen: a tool call, a Cypher query, a fetched page, a probability. This document lists every feature we use, where it lives in the code, and what you see when it runs.

```
you type ─▶ TrueForge agent edits the rubric (MCP tools)
          ─▶ FalkorDB: criterion → topics → shortestPath to candidates (+ full-text)
          ─▶ System One scores each candidate × criterion
          ─▶ uncertain cells become gaps ─▶ System One picks a Bright Data strategy
          ─▶ Bright Data fetches evidence ─▶ System One rescores ─▶ FalkorDB stores it
          ─▶ FalkorDB shortestPath from you to each top pick is the "because"
type again mid-turn ─▶ the turn is cancelled and re-planned with the new context
```

---

## FalkorDB — the datastore, the search, and the explanation

FalkorDB holds the whole conference as a graph, and the app's search and reasons are graph operations.

### Graph model

| Node | What it is |
|---|---|
| `Entity {kind:'sponsor'}` | 54 GitHub Universe sponsors (tier, description, logo, discovered website) |
| `Entity {kind:'talk'}` | 157 sessions (time, room, abstract, speakers) |
| `Entity {kind:'speaker'}` / `Entity:Person {kind:'person'}` | featured speakers and 175 presenters you can meet |
| `Topic` | 20 subject topics + 8 role topics (academic, author, recruiter, …) |
| `User`, `Criterion` | each attendee's live rubric |
| `Evidence` | every Bright Data fetch that changed a score |
| `Source` | the official page each record was scraped from |

| Edge | Meaning |
|---|---|
| `LISTED_ON` | entity → official GitHub Universe page it came from (provenance) |
| `PRESENTS` / `PRESENTED_BY` | person ↔ talk |
| `WORKS_AT` / `EMPLOYS` | presenter ↔ sponsor company (29 links) |
| `ABOUT` / `COVERS` | entity ↔ topic, weighted by classification probability |
| `WANTS {weight}` | user → criterion |
| `MAPS_TO {p}` | criterion → topic |
| `SCORED {p}` | entity → criterion: System One's score, reused as memory |
| `ABOUT` / `INFORMS` (from `Evidence`) | evidence → entity it describes, and criterion it informed |

Directed "route" edges (`COVERS`, `PRESENTED_BY`, `EMPLOYS`) all point away from the attendee, so a single directed `shortestPath` walks from *you* to any recommendation. FalkorDB doesn't support undirected `shortestPath`.

### Features and the queries that use them

**`shortestPath` for search** (`engine.py`, `score_new`). Every entity reachable from a new criterion within 5 hops is a candidate. Path length and edge weights rank them:
```cypher
MATCH (c:Criterion {id:$cid}), (e:Entity)
WITH c, e, shortestPath((c)-[:MAPS_TO|COVERS|PRESENTED_BY|WORKS_AT|EMPLOYS|PRESENTS*..5]->(e)) AS p
WHERE p IS NOT NULL
RETURN e.id, length(p), reduce(w = 1.0, r IN relationships(p) | w * coalesce(r.p, 0.6)) AS w
ORDER BY length(p), w DESC LIMIT 60
```
This finds things keyword search can't. "Speeds up CI/CD builds" reaches **Port's booth** through *criterion → CI/CD → a CI/CD talk → its presenter → works at Port*.

**`shortestPath` as the explanation** (`engine.py`, `explain`). The "because" line under each top recommendation is your path to it. The same path is highlighted in the graph panel:
```cypher
MATCH (u:User {id:$sid}), (e:Entity {id:$id})
WITH u, e, shortestPath((u)-[:WANTS|MAPS_TO|COVERS|PRESENTED_BY|WORKS_AT|EMPLOYS|PRESENTS*..7]->(e)) AS p
WHERE p IS NOT NULL
RETURN [n IN nodes(p) | [labels(n)[0], coalesce(n.id, ''), coalesce(n.name, n.text, '')]], [r IN relationships(p) | type(r)]
```

**Full-text index** (`ingest.py` creates it, `engine.py` queries it). This adds keyword precision on top of the graph routes:
```cypher
CALL db.idx.fulltext.createNodeIndex('Entity', 'name', 'description')
CALL db.idx.fulltext.queryNodes('Entity', $q) YIELD node, score RETURN node.id, score ORDER BY score DESC LIMIT 5
```

**Range indexes** on `Entity.id`, `Criterion.id` and `Person.id` for the per-request lookups.

**Agent memory.** The rubric, scores and evidence persist in the graph:
- *Rubric:* `MERGE (u:User {id:$sid}) MERGE (c:Criterion {id:$cid}) … MERGE (u)-[r:WANTS]->(c) SET r.weight=$w`. Reweight and remove are single-edge updates.
- *Score memory:* a criterion that comes back is recalled, not recomputed:
  ```cypher
  MATCH (e:Entity)-[s:SCORED]->(c:Criterion) WHERE c.id IN $ids
  OPTIONAL MATCH (x:Evidence)-[:ABOUT]->(e) WHERE (x)-[:INFORMS]->(c)
  RETURN e.id, c.id, s.p, s.ev, x.product, x.url, x.snippet
  ```
- *Evidence:* each Bright Data result that changes a score is written as a node, linked to what it informed:
  ```cypher
  MATCH (e:Entity {id:$e}), (c:Criterion {id:$c}) MERGE (e)-[s:SCORED]->(c) SET s.p=$p, s.ev=1
  CREATE (x:Evidence {url:$u, product:$prod, snippet:$snip, at:timestamp()})
  CREATE (x)-[:ABOUT]->(e) CREATE (x)-[:INFORMS]->(c)
  ```
- *Strategy memory:* a sponsor website found by search is saved (`SET e.url=$u`), so the next gap about that sponsor reads it directly.

**Multi-hop detail queries** (`server.py`, `/api/entity`, shown when you click a recommendation):
- Sponsor → people from that company → their talks: `MATCH (p:Person)-[:PRESENTS]->(t:Entity {kind:'talk'}) WHERE toLower(p.org) CONTAINS toLower($org) …`
- Talk → presenters → their other talks: `MATCH (p:Person)-[:PRESENTS]->(t:Entity {id:$id}) OPTIONAL MATCH (p)-[:PRESENTS]->(o:Entity) …`
- Provenance: `MATCH (e:Entity {id:$id})-[:LISTED_ON]->(src:Source) RETURN src.url, src.product`

**Live graph view** (`/api/graph`): you → criteria → topics → recommendations → people → evidence, queried after every write. You can zoom, drag and click it.

Every query that runs shows in the trace panel with its latency.

---

## Bright Data — filling the gaps, with a different strategy for each kind of gap

Bright Data is used two ways: to build the dataset, and to answer questions the graph can't. The choice between its products is itself a decision, made by a decision model for each gap.

### Building the graph (`ingest.py`)
| Product | Used for | Why this product |
|---|---|---|
| **Browser API** (`mcp_browser` zone, CDP via Playwright) | GitHub Universe sponsor portal: 54 sponsors, tiers, descriptions, logo tiles. Session catalog: clicks **Show more** until all 157 sessions load, then reads times, rooms, abstracts and speakers | The portal (RainFocus) is JavaScript-only. A plain fetch returns an empty 1.9KB shell |
| **Web Unlocker** (`mcp_unlocker` zone, `/request`, markdown) | githubuniverse.com home page: featured speakers and headshots | Static page, so the cheapest product works |

### Filling gaps at runtime (`engine.py`)
A cell becomes a gap when checking it could change the ranking: a high-weight criterion with an uncertain probability on a top contender, or a person or speaker whose record is too thin to judge. For each gap, **nimble-v3 makes a `choice`** over the strategies that still make sense:

| Strategy | Bright Data product | When it fits |
|---|---|---|
| `web_search` | **SERP API**: Google results as JSON (`brd_json=1`, `gl=us`) through Web Unlocker | The answer is probably on third-party pages |
| `read_site` | **SERP API** to find the official domain if unknown, then **Web Unlocker** to read it as markdown | Vendor capabilities the company states itself |
| `render_browser` | **Browser API** | Only after `read_site` came back empty or JavaScript-only |
| `people_search` | **SERP API** with the person's name and the topic | Who a person is: talks, courses, blog posts |

- **Escalation:** if a strategy returns nothing relevant, it's marked as tried, and the router picks again from what's left (search → read site → render).
- **On screen:** a gap shows as a striped cell. Then you see the router's probability for each strategy, then the live fetch ("reading chainguard.dev", "SERP API: …"). Finally the bar jumps, and the cell cites its source and shows the quote.
- **Evidence filtering:** search results only count as evidence if they mention the entity, and the citation is the result the quote came from.

---

## TrueFoundry — TrueForge agent harness

The agent that turns what you say into rubric edits runs on **TrueForge**, locally (`npx @truefoundry/trueforge`).

| Feature | How it's used |
|---|---|
| **Custom model provider** | `truefoundry_setup.py` registers our self-hosted qwen3.8 (OpenAI-compatible) as `stratcon-qwen/qwen3-8`, so no OpenAI key is needed |
| **MCP servers** | StratCon exposes its actions as an MCP server (`mcp_tools.py`, `/tools/mcp`, protected by a shared-secret header): `get_rubric`, `add_criterion`, `remove_criterion`, `reweight`, `set_scope`, `set_clock`, `export_calendar`. It's registered with TrueForge as a remote MCP server |
| **Tool discovery** | TrueForge finds and calls tools through its system tools. Each step appears as a chip: `list_tools("stratcon")` → `get_tool_info("stratcon", "add_criterion")` → `call_tool → stratcon.add_criterion(...)` ✓ |
| **Sessions and streamed turns** | Each attendee gets a TrueForge session. Each message is a streamed turn: we read `model.message.delta` (tool-call fragments, reassembled), `tool.response` and `turn.done` |
| **Cancellation** | Typing while a turn runs calls `sessions.cancel` and starts a new turn with the full conversation as context. This is the redirect feature |
| **Outbound URL guard** | TrueForge blocks private hosts by default. We allow only our local MCP server through `OUTBOUND_URL_ALLOWED_HOSTS` |
| **Calendar export as a tool** | `export_calendar` builds an `.ics` file of the best non-overlapping talks plus an expo-hall block for the top booths |

---

## System One decision models (Ollama `/v1/systemone`)

These aren't a sponsor, but they make the decisions. Calls are batched, at most a few questions per call.

| Decision | Model | Type |
|---|---|---|
| Which topics and roles a criterion maps to | nimble-v3 | `choice` |
| Classifying entities into topics and people into roles (ingest) | nimble-v3 / clef-flash | `choice` |
| Does entity X satisfy criterion Y? | clef-flash (calibrated) | `noul` |
| Which Bright Data strategy fills this gap? | nimble-v3 | `choice` |
| Does the new evidence specifically show it? | clef-flash | `noul` |

---

## Qodo

All work lands through pull requests on `kristopolous/stratcon` for Qodo review.
