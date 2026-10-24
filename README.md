# StratCon

**Strategize your conference attendance.** Tell it what you're working on; it tells you which sponsors and speakers at GitHub Universe 2026 to see, and why — and when it isn't sure, it goes and finds out.

Built at Patch the Conference (2026-10-24).

## What happens when you type

1. **An agent edits your rubric.** Your message becomes tool calls — `add_criterion`, `reweight`, `remove_criterion`, `set_scope` — on a short list of checkable criteria. Change your mind and the rubric changes; the ranking follows.
2. **A decision model shortlists the whole hall in one pass.** `nimble-v3` runs a single `choice` over all 62 sponsors and speakers per criterion.
3. **A decision model scores every cell.** `clef-flash` answers each sponsor × criterion as a calibrated `noul` probability, a few questions per call, streamed into the board.
4. **It finds the gaps.** Cells where checking could change the ranking are flagged.
5. **A decision model picks how to search.** For each gap, `nimble-v3` chooses a Bright Data strategy — Google via SERP API, the official site via Web Unlocker, a full render via Browser API, or a person's own work — and escalates if the cheap one comes back empty.
6. **Evidence rescores the cell.** The snippet and its source URL go back to `clef-flash`; the bar moves, the ranking moves.
7. **FalkorDB remembers.** Rubric, scores, discovered websites and evidence live in the graph, so a criterion that comes back is recalled instead of recomputed, and every recommendation's "because" is a path to a cited source.

Nothing on screen is staged: every number is the output of a call that ran, with its real latency and source.

## Data

`ingest.py` pulls the real GitHub Universe 2026 sponsor list (54 sponsors, rendered with Bright Data Browser API because the portal is JavaScript-only) and featured speakers (Web Unlocker) into FalkorDB.

## Run

```bash
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
cp .env.example .env   # fill in FalkorDB, Bright Data, System One and LLM endpoints
./venv/bin/python ingest.py                 # load sponsors and speakers
./venv/bin/python ingest.py --reset-memory  # optional: cold-start the graph memory
./venv/bin/python server.py                 # http://localhost:8000
```

## Files

- `engine.py` — the turn pipeline; yields one event per real call
- `brightdata.py` — SERP API, Web Unlocker, Browser API
- `ingest.py` — conference data into FalkorDB
- `server.py` — FastAPI, streams events to the page
- `static/index.html` — the board, the graph, the trace
