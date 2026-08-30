#!/usr/bin/env python3
"""Discover how many articles each journal actually holds, so runs become verifiable.

The completeness gate in run_pipeline can only judge a seed that declares an
expected count, and as of 2026-08-29 exactly 1 of 401 domains in the run history
did. This probes each journal for that number using the cheapest reliable source
per platform, falling back to a rendered page only when the listing is JS-built.

Deliberately conservative: a wrong count creates false SHORT alarms, which would
train everyone to ignore the gate. Anything uncertain is recorded as unknown with
the reason, never guessed.

Politeness: one request at a time, per-host Crawl-delay honoured (floor of
--min-delay), robots.txt Disallow respected -- a disallowed probe is skipped, not
worked around. Resumable: already-probed seeds are skipped on restart.

Usage:
  python scripts/quality/backfill_expected_counts.py --out results.jsonl [--limit N]
"""
from __future__ import annotations

import argparse, glob, json, os, re, signal, sys, time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import requests

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36")
COUNT_KEYS = ("expected_pdfs", "articles_observed", "pdfs_found")

# Only these methods enumerate the whole journal. Everything else samples one page
# or one post type, and an expected count that is too SMALL is worse than none: a
# truncated run would then compare favourably and report OK, hiding the truncation
# the gate exists to catch.
HIGH_CONFIDENCE = ("dc_sitemap", "dspace_totalElements", "wp_x_total")

session = requests.Session(); session.headers.update({"User-Agent": UA})
_robots_lock = None


class SeedTimeout(Exception):
    pass


class seed_deadline:
    """Hard upper bound on one seed's probing.

    requests' timeout is per socket operation, so a server that accepts a
    connection and then never speaks can hold a worker indefinitely - the same
    unbounded-wait shape that stalled a crawl for 45 minutes earlier today. One
    pathological host must not be able to stall a 1,342-seed job.
    """

    def __init__(self, seconds: int) -> None:
        self.seconds = seconds

    def __enter__(self):
        def _fire(signum, frame):
            raise SeedTimeout()
        self._old = signal.signal(signal.SIGALRM, _fire)
        signal.alarm(self.seconds)
        return self

    def __exit__(self, *exc):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self._old)
        return False


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

_robots: Dict[str, Tuple[List[Tuple[bool, str]], float]] = {}
_last_hit: Dict[str, float] = {}


def _parse_robots(text: str, floor: float) -> Tuple[List[Tuple[bool, str]], float]:
    """Collect the rules that apply to us, plus any Crawl-delay.

    urllib's RobotFileParser is first-match-wins, but RFC 9309 says the LONGEST
    matching rule wins. That difference is not academic: openyls.law.yale.edu
    writes `Allow: /server/api` before `Disallow: /server/api/`, and first-match
    reports the disallowed search API as crawlable. Under-blocking is the one
    error a politeness layer must not make, so match length explicitly.
    """
    rules: List[Tuple[bool, str]] = []
    delay = floor
    applies = False
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        field, _, value = line.partition(":")
        field = field.strip().lower(); value = value.strip()
        if field == "user-agent":
            applies = value == "*"
        elif applies and field in ("allow", "disallow") and value:
            rules.append((field == "allow", value))
        elif applies and field == "crawl-delay":
            try: delay = max(delay, float(value))
            except ValueError: pass
    return rules, delay


def _rule_re(pattern: str) -> "re.Pattern[str]":
    """robots pattern -> anchored regex. `*` is any run, a trailing `$` ends the match."""
    end = pattern.endswith("$")
    body = pattern[:-1] if end else pattern
    rx = "".join(".*" if ch == "*" else re.escape(ch) for ch in body)
    return re.compile("^" + rx + ("$" if end else ""))


def _can_fetch(rules: List[Tuple[bool, str]], target: str) -> bool:
    """Longest matching rule wins (RFC 9309); ties go to Allow.

    `target` is path+query, because patterns routinely constrain the query -- Yale
    disallows `/collections/*?f` (faceted views) while leaving `/collections/<id>`
    crawlable, and matching on path alone would wrongly block the whole section.
    """
    best_len, best_allow = -1, True
    for is_allow, pattern in rules:
        if not _rule_re(pattern).match(target):
            continue
        if len(pattern) > best_len or (len(pattern) == best_len and is_allow):
            best_len, best_allow = len(pattern), is_allow
    return best_allow


def robots_for(origin: str, floor: float) -> Tuple[Optional[List[Tuple[bool, str]]], float]:
    if origin in _robots:
        return _robots[origin]
    rules: Optional[List[Tuple[bool, str]]] = None
    delay = floor
    try:
        r = session.get(urljoin(origin, "/robots.txt"), timeout=(15, 30))
        if r.status_code == 200:
            rules, delay = _parse_robots(r.text, floor)
    except requests.RequestException:
        pass
    _robots[origin] = (rules, delay)
    return rules, delay


def allowed(url: str, floor: float) -> Tuple[bool, float]:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    rules, delay = robots_for(origin, floor)
    if not rules:
        return True, delay
    target = (parsed.path or "/") + (f"?{parsed.query}" if parsed.query else "")
    return _can_fetch(rules, target), delay


def get(url: str, floor: float, **kw) -> Optional[requests.Response]:
    ok, delay = allowed(url, floor)
    if not ok:
        return None
    # Pace per host, not globally: back-to-back seeds are different journals on
    # different servers, and a global sleep would spend the budget waiting on a
    # host we are not about to contact.
    host = urlparse(url).netloc
    wait = delay - (time.time() - _last_hit.get(host, 0.0))
    if wait > 0:
        time.sleep(wait)
    _last_hit[host] = time.time()
    try:
        return session.get(url, timeout=(15, 45), **kw)
    except requests.RequestException:
        return None


# ---- per-platform probes: each returns (count, method) or (None, reason) ----

def probe_digital_commons(seed: str, floor: float):
    """bepress publishes a per-series sitemap listing every article page."""
    parsed = urlparse(seed)
    series = (parsed.path.rstrip("/").split("/") or [""])[-1] or ""
    base = f"{parsed.scheme}://{parsed.netloc}"
    for candidate in (f"{base}/{series}/sitemap.xml", f"{base}/sitemap.xml"):
        r = get(candidate, floor)
        if not r or r.status_code != 200 or "<loc>" not in r.text:
            continue
        locs = re.findall(r"<loc>([^<]+)</loc>", r.text)
        arts = [u for u in locs if re.search(r"/vol\d+/iss\d+/\d+", u)]
        if arts:
            return len(set(arts)), f"dc_sitemap:{candidate.rsplit('/',2)[-2]}"
    return None, "dc_sitemap_unavailable"


def probe_dspace(seed: str, floor: float):
    """DSpace reports the collection size in the search API's page.totalElements."""
    parsed = urlparse(seed)
    m = re.search(r"/(?:collections|handle)/([0-9a-f-]{36})", seed)
    if not m:
        return None, "dspace_no_scope"
    url = f"{parsed.scheme}://{parsed.netloc}/server/api/discover/search/objects"
    ok, _ = allowed(url, floor)
    if not ok:
        return None, "dspace_api_robots_disallowed"
    r = get(url, floor, params={"scope": m.group(1), "size": 1})
    if not r or r.status_code != 200:
        return None, "dspace_api_error"
    try:
        page = r.json()["_embedded"]["searchResult"]["page"]
        return int(page["totalElements"]), "dspace_totalElements"
    except (ValueError, KeyError, TypeError):
        return None, "dspace_parse_failed"


def probe_wordpress(seed: str, floor: float):
    """WP exposes the post count for a type in the X-WP-Total response header."""
    parsed = urlparse(seed)
    base = f"{parsed.scheme}://{parsed.netloc}"
    # Only journal-specific post types. Generic /posts counts blog entries and
    # yields a confidently wrong, far-too-small number.
    for path in ("/wp-json/wp/v2/issues", "/wp-json/wp/v2/article",
                 "/wp-json/wp/v2/articles", "/wp-json/wp/v2/issuescategory"):
        r = get(f"{base}{path}", floor, params={"per_page": 1})
        if r is not None and r.status_code == 200 and r.headers.get("X-WP-Total"):
            try:
                total = int(r.headers["X-WP-Total"])
            except ValueError:
                continue
            if total > 0:
                return total, f"wp_x_total:{path.rsplit('/',1)[-1]}"
    return None, "wp_rest_unavailable"


def probe_rendered(seed: str, floor: float, browser) -> Tuple[Optional[int], str]:
    """Last resort: render the listing and count same-origin PDF links.

    Only used when the static HTML has no links, i.e. a JS-built listing.
    A floor is applied because a rendered page that shows one screen of results
    would otherwise report a confidently wrong, far-too-small number.
    """
    ok, delay = allowed(seed, floor)
    if not ok:
        return None, "robots_disallowed"
    time.sleep(delay)
    page = browser.new_page(user_agent=UA)
    try:
        page.goto(seed, timeout=60000, wait_until="networkidle")
        hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
    except Exception as exc:
        return None, f"render_failed:{type(exc).__name__}"
    finally:
        page.close()
    host = urlparse(seed).netloc
    pdfs = {h for h in hrefs if h.lower().split("?")[0].endswith(".pdf")
            and urlparse(h).netloc == host}
    if len(pdfs) < 5:
        return None, f"rendered_too_few:{len(pdfs)}"
    return len(pdfs), "rendered_first_page_only"


def platform_of(meta: Dict[str, Any], seed: str) -> str:
    p = str(meta.get("platform") or "").lower()
    if "digital" in p or "bepress" in p or "/vol" in seed:
        return "digitalcommons"
    if "dspace" in p or "/collections/" in seed:
        return "dspace"
    if "wordpress" in p:
        return "wordpress"
    return p or "unknown"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seeds-dir", default="offprint/sitemaps")
    ap.add_argument("--out", default="artifacts/expected_counts.jsonl")
    ap.add_argument("--min-delay", type=float, default=6.0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--seed-timeout", type=int, default=120,
                    help="hard cap on one seed's probing (default: 120s)")
    ap.add_argument("--render", action="store_true",
                    help="also render JS listings (slow; yields only observed_partial, "
                         "never an expected count)")
    ap.add_argument("--no-render", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    done = set()
    if os.path.exists(args.out):
        for line in open(args.out, encoding="utf-8"):
            try: done.add(json.loads(line)["seed_file"])
            except Exception: pass

    todo = []
    for path in sorted(glob.glob(os.path.join(args.seeds_dir, "*.json"))):
        name = os.path.basename(path)
        if name in done:
            continue
        try: payload = json.loads(open(path, encoding="utf-8").read())
        except (OSError, ValueError): continue
        meta = payload.get("metadata") or {}
        nav = meta.get("navigation") or {}
        if any(isinstance(nav.get(k), int) and nav[k] > 0 for k in COUNT_KEYS):
            continue
        if str(meta.get("status") or "").lower().startswith(("paused", "dead", "superseded")):
            continue
        urls = payload.get("start_urls") or []
        if not urls:
            continue
        todo.append((name, str(urls[0]).replace("[", "1").replace("]", ""), meta))
    if args.limit:
        todo = todo[:args.limit]
    log(f"{len(done)} already probed; {len(todo)} to probe")

    browser = pw = None
    if args.render and not args.no_render:
        try:
            from playwright.sync_api import sync_playwright
            pw = sync_playwright().start()
            browser = pw.chromium.launch(headless=True)
            log("playwright chromium ready for JS-built listings")
        except Exception as exc:
            log(f"playwright unavailable ({type(exc).__name__}); static probes only")

    found = 0
    try:
        with open(args.out, "a", encoding="utf-8") as out:
            for i, (name, seed, meta) in enumerate(todo, 1):
                plat = platform_of(meta, seed)
                probe = {"digitalcommons": probe_digital_commons,
                         "dspace": probe_dspace,
                         "wordpress": probe_wordpress}.get(plat)
                count = method = None
                try:
                    with seed_deadline(args.seed_timeout):
                        if probe:
                            count, method = probe(seed, args.min_delay)
                        if count is None:
                            for fb in (probe_digital_commons, probe_wordpress, probe_dspace):
                                if fb is probe:
                                    continue
                                count, method = fb(seed, args.min_delay)
                                if count:
                                    break
                        if count is None and browser is not None:
                            count, method = probe_rendered(seed, args.min_delay, browser)
                except SeedTimeout:
                    count, method = None, f"timeout_{args.seed_timeout}s"
                    log(f"  timeout on {name[:44]} - moving on")
                conf = ("high" if method and method.startswith(HIGH_CONFIDENCE)
                        else ("low" if count else "none"))
                if conf == "low":
                    # keep the observation, but do not present it as a total
                    count_field, observed = None, count
                else:
                    count_field, observed = count, None
                out.write(json.dumps({
                    "seed_file": name, "seed_url": seed, "platform": plat,
                    "expected": count_field, "observed_partial": observed,
                    "confidence": conf, "method": method,
                    "journal": meta.get("journal_name"),
                    "probed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }) + "\n"); out.flush()
                if conf == "high":
                    found += 1
                if i % 25 == 0 or conf == "high":
                    log(f"{i}/{len(todo)}  found={found}  {name[:44]} -> {count} ({method})")
    finally:
        if browser: browser.close()
        if pw: pw.stop()
    log(f"DONE probed={len(todo)} high_confidence_counts={found} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
