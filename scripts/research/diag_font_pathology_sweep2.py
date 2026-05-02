"""Refined sweep: cmap-marker character frequencies + zero-text doc detector."""
from __future__ import annotations
import json, os, re, sys, statistics
sys.path.insert(0, "/mnt/shared_storage/law-review-corpus/offprint")
from offprint.pdf_footnotes.text_extract import _load_liteparse_page_layouts
from collections import Counter

ACCENTED = re.compile(r"[áéíóúñÁÉÍÓÚÑüÜçÇßàâêîôûÀÂÊÎÔÛèÈùÙ]")
# Latin-1 supplement printable chars EXCLUDING accented Latin letters
# Cmap-mangled glyphs in our corpus appear primarily as single-character ASCII
# symbols that wouldn't naturally appear standalone in body text:
CMAP_MARKERS = set("!#$%*+")
# Replacement chars / C1 controls (always pathological):
CTRL_REPL = re.compile(r"[-�]")

def feats(pdf):
    layouts = _load_liteparse_page_layouts(pdf)
    if not layouts:
        return None
    n_pages = len(layouts)
    items_total = 0
    accented = 0
    cmap_marker_tokens = 0
    ctrl_repl_tokens = 0
    sizes = []
    page_zero = 0
    for layout in layouts:
        if len(layout.lines) == 0:
            page_zero += 1
        for it in layout.raw_items or ():
            t = str(it.get("text") or "")
            sz = float(it.get("fontSize") or 0.0)
            if sz > 0:
                sizes.append(sz)
            items_total += 1
            if ACCENTED.search(t):
                accented += 1
            if len(t) == 1 and t in CMAP_MARKERS:
                cmap_marker_tokens += 1
            if CTRL_REPL.search(t):
                ctrl_repl_tokens += 1
    median_sz = statistics.median(sizes) if sizes else 0.0
    return dict(
        pages=n_pages, items=items_total, median_size=median_sz,
        zero_pages=page_zero, zero_frac=page_zero/max(1,n_pages),
        accented_frac=accented/max(1,items_total),
        cmap_marker_frac=cmap_marker_tokens/max(1,items_total),
        ctrl_repl_frac=ctrl_repl_tokens/max(1,items_total),
    )

def detect(f):
    pages = f['pages']
    if pages < 3:
        return '-'
    # Safeguard: non-English doc (Spanish, etc.)
    if f['accented_frac'] >= 0.01:
        return '-'
    # Pathology A: total no text extracted (image scan / broken)
    if f['items'] == 0 and pages >= 3:
        return 'NO_TEXT'
    # Pathology B: cmap mangling — many single-char markers
    if f['cmap_marker_frac'] >= 0.005:
        return 'CMAP'
    # Pathology C: control/replacement chars
    if f['ctrl_repl_frac'] >= 0.01:
        return 'CTRL'
    # Pathology D: most pages produce zero clustered lines despite raw text
    if f['zero_frac'] >= 0.30 and f['items'] > 0:
        # require corroborating: median font size anomaly OR many cmap markers
        if f['median_size'] >= 30:
            return 'FONT_INFL'
    return '-'

data = json.load(open('artifacts/runs/holdout_1k_after_all_fixes.json'))
rows = data['rows']
import random
random.seed(0)
valid = [r for r in rows if r.get('status') == 'valid']
sampled_valid = random.sample(valid, 100)

target = (
    [('invalid', r) for r in rows if r.get('status') == 'invalid']
    + [('empty', r) for r in rows if r.get('status') == 'empty']
    + [('valid', r) for r in sampled_valid]
)

results = []
for status, r in target:
    pdf = r.get('pdf')
    if not pdf or not os.path.exists(pdf):
        continue
    try:
        f = feats(pdf)
    except Exception as e:
        continue
    if not f:
        continue
    f['status'] = status
    f['pdf'] = pdf
    f['doc_type'] = r.get('doc_type', '-')
    results.append(f)

ct = Counter()
fps = []
for f in results:
    flag = detect(f)
    ct[(f['status'], flag)] += 1
    if f['status'] == 'valid' and flag != '-':
        fps.append((flag, f))

print("=== Flag counts by status ===")
for k, v in sorted(ct.items()):
    print(k, v)

print("\n=== False positives on valid docs ===")
for flag, f in fps:
    print(f"  {flag} pg={f['pages']} items={f['items']} cmap_frac={f['cmap_marker_frac']:.4f} acc={f['accented_frac']:.4f} {os.path.basename(f['pdf'])}")

print("\n=== True positives by detector ===")
ct2 = Counter()
for f in results:
    flag = detect(f)
    if flag != '-':
        ct2[(f['status'], flag)] += 1
        if f['status'] in ('empty','invalid'):
            print(f"  TP {flag} pg={f['pages']} items={f['items']} cmap={f['cmap_marker_frac']:.4f} zfrac={f['zero_frac']:.2f} {os.path.basename(f['pdf'])}")
