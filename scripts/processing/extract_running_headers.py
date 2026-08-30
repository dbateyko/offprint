#!/usr/bin/env python3
"""Derive volume/year/journal-identity from running headers for Jurimetrics + OSTLJ."""
import json, os, re, sys, glob
from concurrent.futures import ProcessPoolExecutor

SEASON = re.compile(r"^(SPRING|SUMMER|FALL|AUTUMN|WINTER)\s+((?:19|20)\d{2})\s*$", re.I)
JURI_VOL = re.compile(r"\b(\d{1,3})\s+JURIMETRICS\b", re.I)
IS_VOL = re.compile(r"\[\s*Vol\.\s*(\d{1,3})\s*:\s*(\d{1,3})", re.I)
IS_YEAR = re.compile(r"^\s*((?:19|20)\d{2})\s*\]")
IS_NAME = re.compile(r"I\s*/\s*S\s*:\s*A\s+JOURNAL\s+OF\s+LAW\s+AND\s+POLICY", re.I)


def lines_of(doc, i):
    return [l.strip() for l in doc[i].get_text().split("\n") if l.strip()]


def jurimetrics(path):
    import pymupdf
    out = {"path": path, "kind": "jurimetrics"}
    try:
        d = pymupdf.open(path)
    except Exception as e:
        out["error"] = f"open_failed: {e}"
        return out
    try:
        out["pages"] = len(d)
        if len(d) == 0:
            out["error"] = "zero_pages"
            return out
        p0 = lines_of(d, 0)
        # journal identity + volume from running heads on any of pages 2-6
        for i in range(1, min(6, len(d))):
            for l in lines_of(d, i)[:6]:
                m = JURI_VOL.search(l)
                if m:
                    out["volume"] = m.group(1)
                    out["journal_evidence"] = l
                    break
            if out.get("volume"):
                break
        # season/year + start page + title from p0
        idx = 0
        if p0 and SEASON.match(p0[0]):
            m = SEASON.match(p0[0])
            out["season"] = m.group(1).upper()
            out["year"] = m.group(2)
            idx = 1
            if idx < len(p0) and re.fullmatch(r"\d{1,4}", p0[idx]):
                out["start_page"] = p0[idx]
                idx += 1
        # title = consecutive mostly-uppercase lines
        title_parts = []
        while idx < len(p0):
            l = p0[idx]
            letters = [c for c in l if c.isalpha()]
            if letters and sum(c.isupper() for c in letters) / len(letters) > 0.85 and len(l) > 2:
                title_parts.append(l)
                idx += 1
            else:
                break
        if title_parts:
            out["title"] = " ".join(title_parts).rstrip(":* ")
        # author = next non-empty line, strip footnote markers
        if idx < len(p0):
            cand = p0[idx]
            cand = re.sub(r"[*†‡§¶∗✦]+", "", cand).strip().rstrip(",. ")
            if cand and len(cand) < 120 and re.match(r"^[A-Z]", cand):
                out["author_line"] = cand
        out["p0_head"] = p0[:8]
    finally:
        d.close()
    return out


def ostlj(path):
    import pymupdf
    out = {"path": path, "kind": "ostlj"}
    try:
        d = pymupdf.open(path)
    except Exception as e:
        out["error"] = f"open_failed: {e}"
        return out
    try:
        out["pages"] = len(d)
        if len(d) == 0:
            out["error"] = "zero_pages"
            return out
        for i in range(0, min(8, len(d))):
            ls = lines_of(d, i)[:6]
            blob = " ".join(ls)
            if IS_NAME.search(blob):
                out["journal_evidence"] = blob[:120]
                m = IS_VOL.search(blob)
                if m:
                    out["volume"], out["issue"] = m.group(1), m.group(2)
            for l in ls:
                m = IS_YEAR.match(l)
                if m and "year" not in out:
                    out["year"] = m.group(1)
            if out.get("volume") and out.get("year"):
                break
    finally:
        d.close()
    return out


def work(args):
    kind, path = args
    return jurimetrics(path) if kind == "jurimetrics" else ostlj(path)


if __name__ == "__main__":
    kind = sys.argv[1]
    root = sys.argv[2]
    outp = sys.argv[3]
    files = sorted(glob.glob(os.path.join(root, "*.pdf")))
    tasks = [(kind, f) for f in files]
    res = []
    with ProcessPoolExecutor(max_workers=6) as ex:
        for r in ex.map(work, tasks, chunksize=4):
            res.append(r)
    with open(outp, "w") as fh:
        for r in res:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"{len(res)} written to {outp}")
