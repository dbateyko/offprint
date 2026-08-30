#!/usr/bin/env python3
"""Enumerate every document in a Quartex journal and write a ready-to-run seed.

Quartex listings are Knockout.js: the server HTML carries no article links, so a
static crawl finds nothing and the journals look unreachable. They are not. The
page fetches its results from `document/getdocumentlist`, which filters by
collection name and reports totalRecords -- the whole journal, exactly.

Rather than guess that API's shape (reconstructing it from the JS bundle returns
HTTP 500), this drives a real page, captures the request template and bearer
token the app itself sends, then pages through from inside the page so auth rides
along. The resulting Detail URLs are written into a seed as start_urls, which
QuartexAdapter already downloads correctly, plus navigation.expected_pdfs so the
completeness gate can verify the run afterwards.

Usage:
  python scripts/onboarding/enumerate_quartex.py \
      --journal "https://repository.law.upenn.edu/journal-of-business-law/jbl" \
      --seed-out offprint/sitemaps/repository-law-upenn-edu-jbl.json
"""
from __future__ import annotations

import argparse, json, sys, time
from typing import Any, Dict, List, Optional

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36")
PAGE_SIZE = 100


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def enumerate_journal(list_url: str, delay: float) -> Dict[str, Any]:
    from playwright.sync_api import sync_playwright

    captured: Dict[str, Any] = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=UA)

        def on_request(req):
            if "getdocumentlist" in req.url and "template" not in captured:
                captured["template"] = {
                    "url": req.url.split("?")[0],
                    "auth": req.headers.get("authorization", ""),
                    "body": req.post_data,
                }

        page.on("request", on_request)
        page.goto(list_url, timeout=90_000, wait_until="networkidle")
        time.sleep(3)
        if "template" not in captured:
            browser.close()
            raise SystemExit(f"no getdocumentlist request seen on {list_url}")

        tpl = captured["template"]
        body = json.loads(tpl["body"])
        collections = body.get("pagination", {}).get("collections") or []
        log(f"collection filter: {collections}")

        docs: List[Dict[str, Any]] = []
        total = pages = 0
        current = 1
        while True:
            body["pagination"]["pageSize"] = PAGE_SIZE
            body["pagination"]["currentPage"] = current
            res = page.evaluate(
                """async ([u, a, b]) => {
                    const r = await fetch(u, {method:'POST',
                        headers:{'content-type':'application/json','authorization':a},
                        body: JSON.stringify(b)});
                    const j = await r.json();
                    return {status:r.status, total:j.totalRecords, pages:j.totalPages,
                            docs:(j.docs||[]).map(d=>({id:d.documentId||d.id,
                                                       url:d.url||d.documentUrl,
                                                       title:d.title||''}))};
                }""",
                [tpl["url"], tpl["auth"], body],
            )
            if res.get("status") != 200:
                log(f"  page {current}: HTTP {res.get('status')} - stopping")
                break
            total = int(res.get("total") or 0)
            pages = int(res.get("pages") or 0)
            batch = res.get("docs") or []
            docs.extend(batch)
            log(f"  page {current}/{pages}: +{len(batch)} (have {len(docs)}/{total})")
            if current >= pages or not batch:
                break
            current += 1
            time.sleep(delay)           # polite between API pages
        browser.close()

    seen, ordered = set(), []
    for d in docs:
        url = d.get("url") or ""
        if url and url not in seen:
            seen.add(url); ordered.append(d)
    return {"collections": collections, "total_records": total, "docs": ordered}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--journal", required=True, help="journal listing URL")
    ap.add_argument("--seed-out", required=True)
    ap.add_argument("--delay", type=float, default=3.0)
    args = ap.parse_args(argv)

    origin = "/".join(args.journal.split("/")[:3])
    result = enumerate_journal(args.journal, args.delay)
    docs = result["docs"]
    total = result["total_records"]
    log(f"enumerated {len(docs)} unique documents (API reports {total})")
    if total and len(docs) < total * 0.95:
        log(f"WARNING: enumerated well under totalRecords -- not writing a seed")
        return 2

    urls = [origin + d["url"] if d["url"].startswith("/") else d["url"] for d in docs]
    seed_path = args.seed_out
    try:
        seed = json.loads(open(seed_path, encoding="utf-8").read())
    except (OSError, ValueError):
        seed = {"id": seed_path.rsplit("/", 1)[-1].removesuffix(".json"), "metadata": {}}
    meta = seed.setdefault("metadata", {})
    nav = meta.setdefault("navigation", {})
    seed["start_urls"] = urls
    nav["expected_pdfs"] = len(urls)
    nav["pagination"] = ("none - start_urls are enumerated Detail URLs; the Quartex "
                         "listing is Knockout.js and exposes no links in server HTML")
    nav["enumerated_via"] = "document/getdocumentlist (browser-captured template)"
    nav["quartex_collections"] = result["collections"]
    meta["status"] = "active"
    meta["status_reason"] = (f"enumerated {len(urls)} documents via Quartex API "
                             f"on {time.strftime('%Y-%m-%d')}; totalRecords={total}")
    with open(seed_path, "w", encoding="utf-8") as fh:
        json.dump(seed, fh, indent=2)
    log(f"wrote {seed_path} with {len(urls)} start_urls and expected_pdfs={len(urls)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
