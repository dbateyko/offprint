#!/usr/bin/env python3
"""Polite Wayback CDX resolve + raw (id_) download for Jurimetrics article PDFs.

Single worker, >=5s between requests, stops on 403/429/challenge.
For each stem it tries both the `X.pdf` and `X.authcheckdam.pdf` URL forms and
picks the statuscode-200 capture with the LARGEST length (some captures are
truncated at exactly 1 MiB).
"""
import json, os, re, sys, time, urllib.parse, urllib.request
from pathlib import Path

UA = "law-review-corpus research harvester (drb348@cornell.edu)"
DELAY = 5.5
_last = [0.0]


def polite(url, timeout=180):
    dt = time.time() - _last[0]
    if dt < DELAY:
        time.sleep(DELAY - dt)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        body = r.read()
        code = r.getcode()
    except urllib.error.HTTPError as e:
        code, body = e.code, b""
    finally:
        _last[0] = time.time()
    if code in (403, 429, 503):
        raise SystemExit(f"COOLDOWN: got HTTP {code} on {url} -- stopping")
    return code, body


def cdx(original):
    q = ("https://web.archive.org/cdx/search/cdx?url=" + urllib.parse.quote(original, safe="")
         + "&output=json&collapse=digest&limit=200")
    code, body = polite(q)
    if code != 200 or not body.strip():
        return []
    try:
        rows = json.loads(body)
    except Exception:
        return []
    if not rows:
        return []
    hdr = rows[0]
    return [dict(zip(hdr, r)) for r in rows[1:]]


def main():
    inp = json.load(open(sys.argv[1]))
    outdir = Path(sys.argv[2]); outdir.mkdir(parents=True, exist_ok=True)
    log = []
    for stem, urls in inp.items():
        dest = outdir / stem
        entry = {"stem": stem, "candidates": urls, "captures": [], "status": None}
        if dest.exists() and dest.stat().st_size > 0:
            entry["status"] = "already_present"; log.append(entry); continue
        caps = []
        for u in urls:
            for form in (u, re.sub(r"\.pdf$", ".authcheckdam.pdf", u)) if not u.endswith(".authcheckdam.pdf") \
                    else (u, re.sub(r"\.authcheckdam\.pdf$", ".pdf", u)):
                for row in cdx(form):
                    try:
                        ln = int(row.get("length") or 0)
                    except ValueError:
                        ln = 0
                    caps.append({"timestamp": row["timestamp"], "original": row["original"],
                                 "length": ln, "mimetype": row.get("mimetype"),
                                 "status": row.get("statuscode")})
        # 200s first, then biggest length (non-200 captures replay through redirects)
        seen = set(); uniq = []
        for c in sorted(caps, key=lambda c: (c["status"] != "200", -c["length"])):
            k = (c["timestamp"], c["original"])
            if k in seen: continue
            seen.add(k); uniq.append(c)
        entry["captures"] = uniq
        if not uniq:
            entry["status"] = "no_capture"; log.append(entry); print(stem, "NO CAPTURE", flush=True); continue
        for c in uniq[:8]:
            url = f"https://web.archive.org/web/{c['timestamp']}id_/{c['original']}"
            code, body = polite(url)
            note = {"url": url, "http": code, "bytes": len(body)}
            if code == 200 and body[:5] == b"%PDF-" and len(body) != 1048576:
                dest.write_bytes(body)
                entry["status"] = "ok"; entry["chosen"] = c; entry["download"] = note
                print(stem, "OK", len(body), c["timestamp"], flush=True)
                break
            note["reject"] = ("bad_http" if code != 200 else
                              "not_pdf" if body[:5] != b"%PDF-" else "exactly_1MiB_truncated")
            entry.setdefault("attempts", []).append(note)
        else:
            entry["status"] = "all_captures_bad"
            print(stem, "FAILED", entry.get("attempts"), flush=True)
        log.append(entry)
    (outdir / "_fetch_log.json").write_text(json.dumps(log, indent=2))
    print("done")


if __name__ == "__main__":
    main()
