"""Real Bright Data calls. Each returns what happened: product, target, bytes, ms, and the payload."""
import asyncio
import json
import time
import urllib.parse

import httpx
from playwright.async_api import async_playwright

import cache
import config

API = "https://api.brightdata.com"
HDR = {"Authorization": f"Bearer {config.BRIGHT_DATA_KEY}", "Content-Type": "application/json"}
_browser_ws = None


TIMEOUT = 120
TRIES = 3


async def _request(url: str, data_format: str | None) -> tuple[int, str]:
    """One Bright Data /request, reissued when it stalls past TIMEOUT seconds or the service hiccups."""
    body = {"zone": config.UNLOCKER_ZONE, "url": url, "format": "raw"}
    if data_format:
        body["data_format"] = data_format
    last = (0, "no attempt")
    for attempt in range(TRIES):
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT, connect=20)) as c:
                r = await c.post(f"{API}/request", headers=HDR, json=body)
        except (httpx.TimeoutException, httpx.TransportError) as x:
            last = (0, f"{type(x).__name__} after attempt {attempt + 1}")
            continue
        if r.status_code == 429 or r.status_code >= 500:
            last = (r.status_code, r.text)
            await asyncio.sleep(2 * (attempt + 1))
            continue
        return r.status_code, r.text
    return last


def _cached(product: str, target: str, variant: str = ""):
    hit = cache.get("brightdata", cache.key(product, target, variant))
    if hit:
        return {**hit, "cached": True}
    return None


def _store(res: dict) -> dict:
    if res.get("status") == 200 and (res.get("text") or res.get("results")):
        cache.put("brightdata", cache.key(res["product"], res["target"], res.get("variant", "")),
                  {**res, "text": (res.get("text") or "")[:400000]})
    return res


async def unlocker(url: str) -> dict:
    if hit := _cached("Web Unlocker", url):
        return hit
    return _store(await _unlocker(url))


async def _unlocker(url: str) -> dict:
    t = time.time()
    status, text = await _request(url, "markdown")
    return {"product": "Web Unlocker", "strategy": "read_site", "target": url, "status": status,
            "bytes": len(text.encode()), "ms": int((time.time() - t) * 1000), "text": text if status == 200 else ""}


def _real_link(o: dict) -> str:
    link = o.get("link", "")
    if "google.com/goto" not in link and "google.com/url" not in link:
        return link
    shown = o.get("display_link", "")
    if "…" in shown or "..." in shown:
        shown = shown.split(" › ")[0]
    return shown.replace(" › ", "/").strip() or link


async def serp(query: str) -> dict:
    if hit := _cached("SERP API", query):
        return hit
    return _store(await _serp(query))


async def _serp(query: str) -> dict:
    t = time.time()
    url = "https://www.google.com/search?" + urllib.parse.urlencode({"q": query, "gl": "us", "hl": "en", "brd_json": 1})
    status, text = await _request(url, None)
    results = []
    if status == 200:
        try:
            for o in json.loads(text).get("organic", [])[:6]:
                results.append({"title": o.get("title", ""), "link": _real_link(o), "description": o.get("description", "")})
        except ValueError:
            pass
    return {"product": "SERP API", "strategy": "web_search", "target": query, "status": status,
            "bytes": len(text.encode()), "ms": int((time.time() - t) * 1000), "results": results,
            "text": "\n\n".join(f"{r['title']}\n{r['description']}" for r in results)}


async def _ws() -> str:
    global _browser_ws
    if not _browser_ws:
        async with httpx.AsyncClient(timeout=20) as c:
            pw = (await c.get(f"{API}/zone/passwords", params={"zone": config.BROWSER_ZONE}, headers=HDR)).json()["passwords"][0]
            cust = (await c.get(f"{API}/status", headers=HDR)).json()["customer"]
        _browser_ws = f"wss://brd-customer-{cust}-zone-{config.BROWSER_ZONE}:{pw}@brd.superproxy.io:9222"
    return _browser_ws


async def browser(url: str, scroll: int = 2, links_text: str | None = None,
                  click_text: str | None = None, clicks: int = 0, wait_text: str | None = None) -> dict:
    variant = f"scroll={scroll};links={links_text};wait={wait_text}"
    if not clicks and (hit := _cached("Browser API", url, variant)):
        return hit
    for attempt in range(2):
        try:
            res = await asyncio.wait_for(_browser(url, scroll, links_text, click_text, clicks, wait_text),
                                         TIMEOUT if not clicks else TIMEOUT * 2)
            break
        except (asyncio.TimeoutError, httpx.TransportError, ConnectionError):
            if attempt == 1:
                raise
    res["variant"] = variant
    return _store(res) if not clicks else res


async def _browser(url: str, scroll: int = 2, links_text: str | None = None,
                   click_text: str | None = None, clicks: int = 0, wait_text: str | None = None) -> dict:
    t = time.time()
    links, images = [], []
    async with async_playwright() as p:
        b = await p.chromium.connect_over_cdp(await _ws())
        try:
            page = await b.new_page()
            await page.goto(url, timeout=90000, wait_until="domcontentloaded")
            if wait_text:
                await page.get_by_text(wait_text, exact=True).first.wait_for(timeout=60000)
                await page.wait_for_timeout(1500)
            else:
                await page.wait_for_timeout(9000)
            for _ in range(scroll):
                await page.mouse.wheel(0, 4000)
                await page.wait_for_timeout(800)
            items = page.get_by_text(wait_text, exact=True) if wait_text else None
            for _ in range(clicks):
                more = page.get_by_text(click_text, exact=True).first
                if not await more.count():
                    break
                before = await items.count() if items else 0
                for _try in range(3):
                    await more.scroll_into_view_if_needed()
                    await page.wait_for_timeout(800)
                    await more.click(force=True)
                    for _w in range(20):
                        await page.wait_for_timeout(500)
                        if not items or await items.count() > before:
                            break
                    if not items or await items.count() > before:
                        break
            text = await page.inner_text("body")
            if links_text:
                links = await page.eval_on_selector_all(
                    "a", "(els, t) => els.filter(e => e.innerText.trim() === t).map(e => e.href)", links_text)
            images = await page.eval_on_selector_all("img", "els => els.map(e => [e.alt || '', e.src])")
        finally:
            await b.close()
    return {"product": "Browser API", "strategy": "render_browser", "target": url, "status": 200,
            "bytes": len(text.encode()), "ms": int((time.time() - t) * 1000), "text": text, "links": links,
            "images": images}


LINKEDIN_PEOPLE = "gd_l1viktl72bvl7bjuj0"


async def dataset_trigger(dataset_id: str, inputs: list[dict]) -> str:
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{API}/datasets/v3/trigger", params={"dataset_id": dataset_id, "format": "json"},
                         headers=HDR, json=inputs)
        r.raise_for_status()
        return r.json()["snapshot_id"]


async def snapshot_progress(snapshot_id: str) -> dict:
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{API}/datasets/v3/progress/{snapshot_id}", headers=HDR)
        if r.status_code != 200:
            return {"status": "failed", "error": f"HTTP {r.status_code}: {r.text[:160]}"}
        return r.json()


async def snapshot_download(snapshot_id: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(f"{API}/datasets/v3/snapshot/{snapshot_id}", params={"format": "json"}, headers=HDR)
        data = r.json()
        return data if isinstance(data, list) else [data]
