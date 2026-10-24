"""Real Bright Data calls. Each returns what happened: product, target, bytes, ms, and the payload."""
import json
import time
import urllib.parse

import httpx
from playwright.async_api import async_playwright

import config

API = "https://api.brightdata.com"
HDR = {"Authorization": f"Bearer {config.BRIGHT_DATA_KEY}", "Content-Type": "application/json"}
_browser_ws = None


async def _request(url: str, data_format: str | None) -> tuple[int, str]:
    body = {"zone": config.UNLOCKER_ZONE, "url": url, "format": "raw"}
    if data_format:
        body["data_format"] = data_format
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(f"{API}/request", headers=HDR, json=body)
        return r.status_code, r.text


async def unlocker(url: str) -> dict:
    t = time.time()
    status, text = await _request(url, "markdown")
    return {"product": "Web Unlocker", "strategy": "read_site", "target": url, "status": status,
            "bytes": len(text.encode()), "ms": int((time.time() - t) * 1000), "text": text if status == 200 else ""}


async def serp(query: str) -> dict:
    t = time.time()
    url = "https://www.google.com/search?" + urllib.parse.urlencode({"q": query, "gl": "us", "hl": "en", "brd_json": 1})
    status, text = await _request(url, None)
    results = []
    if status == 200:
        try:
            for o in json.loads(text).get("organic", [])[:6]:
                results.append({"title": o.get("title", ""), "link": o.get("link", ""), "description": o.get("description", "")})
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


async def browser(url: str, scroll: int = 2, links_text: str | None = None) -> dict:
    t = time.time()
    links = []
    async with async_playwright() as p:
        b = await p.chromium.connect_over_cdp(await _ws())
        try:
            page = await b.new_page()
            await page.goto(url, timeout=120000)
            await page.wait_for_timeout(5000)
            for _ in range(scroll):
                await page.mouse.wheel(0, 4000)
                await page.wait_for_timeout(800)
            text = await page.inner_text("body")
            if links_text:
                links = await page.eval_on_selector_all(
                    "a", "(els, t) => els.filter(e => e.innerText.trim() === t).map(e => e.href)", links_text)
        finally:
            await b.close()
    return {"product": "Browser API", "strategy": "render_browser", "target": url, "status": 200,
            "bytes": len(text.encode()), "ms": int((time.time() - t) * 1000), "text": text, "links": links}
