# StratCon demo script (~3 minutes)

Each beat says what to do, what appears on screen, and which sponsor feature is doing the work. A turn takes 30 to 60 seconds, so the narration for each step is meant to be said while the activity panel shows it.

---

**[Empty board, three starter cards]**

"When I go to a conference I'm overwhelmed twice: by the sponsors and exhibitors, and by the speakers and tracks. GitHub Universe has 54 sponsor booths, 157 sessions and 183 people presenting. The valuable connections are in there somewhere. I need a *strategy* for the conference."

"All of it was pulled in with **Bright Data**. The sponsor and session portal is JavaScript-only, so a plain fetch gets an empty page. **Browser API** rendered it and clicked 'Show more' until all 157 sessions loaded. **Web Unlocker** read the speakers. All of it lives in **FalkorDB** as a graph: sponsors, talks, people, topics, and who works where."

**[Click "I'm looking for a job."]**

- **TrueForge** (activity panel: *TrueForge agent*). "My words go to an agent running on **TrueForge**. Watch it discover our tools on its own: `list_tools`, `get_tool_info`, then `call_tool → add_criterion`. Those are MCP tools registered with TrueForge, and the model is our own, plugged in through TrueForge's custom model provider."
- **FalkorDB** (*FalkorDB shortestPath*). "The search is **FalkorDB**. My criterion maps onto topic nodes, then a `shortestPath` query walks the graph to every booth, talk and person it can reach. It finds booths through *people*: a talk on the topic, the person presenting it, the company they work for."
- **Decision models** (*System One clef-flash*). "A decision model scores each candidate with a calibrated probability."
- **Bright Data** (striped cells). "Where it isn't sure, a cell becomes a gap, and a decision model picks *which* Bright Data product should find out: **SERP API**, **Web Unlocker** to read the company's site, **Browser API** for JavaScript-heavy pages, or a people search. Watch the bar jump when the evidence lands, with its source cited."

**[Type mid-turn: "no, not that — I mean companies that just raised funding and are hiring engineers"]**

"It took 'job' too generically, so I correct it *while it's working*. It stops, re-plans with what I said, and re-ranks in real time. That's the point: a recommender I steer as it goes."

**[Point at the "because" line under #1, then at the white path in the graph]**

"Every recommendation says why. That line is my actual path through the **FalkorDB** graph, from me to the recommendation, found by `shortestPath` and highlighted in the graph view."

**[Click a topic node in the graph]**

"The graph is live. Clicking a topic runs a **FalkorDB Cypher** query and lists every booth, talk and person on it, not just my top ten, so I can explore the conference through the graph."

**[Click a person on the board]**

"Who is this person? **Bright Data** finds out: SERP finds their profiles, the **LinkedIn people profiles** scraper pulls their company and location, and **Web Unlocker** reads their GitHub for a photo when LinkedIn hides it. **FalkorDB** tells me where they'll be: their talk, and their company's booth through the `WORKS_AT` edge."

**[Pin a pick, then type: "what kind of positions are they hiring for?"]**

"I can question it. That's a question, not a new criterion, so the agent leaves my rubric alone. It researches my picks live through **Bright Data** and can query **FalkorDB** itself in Cypher."

**[Click "Export plan to calendar"]**

"The **TrueForge** agent calls its `export_calendar` tool: the best talks that don't overlap, plus a block for the booths."

**[Type: "It's Wednesday 2pm and I'm at the conference. What now?"]**

"During the conference I tell it the time. The talks that already happened drop off, and it re-plans."

**[Reload the page, click History, open the session]**

"I close the tab and come back later. My whole session is a subgraph in **FalkorDB**: my criteria, every score, the evidence, and the conversation. It comes back in about a third of a second with *zero* AI calls. It's stored, not recomputed."

**[Close]**

"A recommender I can steer, that tells me why, and that I can question. StratCon: built on Bright Data, FalkorDB and TrueForge."

---

## Before going on stage
- `./venv/bin/python ingest.py --reset-memory`, then restart the server.
- TrueForge running locally (`OUTBOUND_URL_ALLOWED_HOSTS='["localhost","127.0.0.1"]' npx @truefoundry/trueforge`).
- Say "no, not that…" for the correction: the agent removes a criterion only on an explicit correction, and otherwise adds.
- Qodo: only mention it once it has actually reviewed a PR on the repo.
