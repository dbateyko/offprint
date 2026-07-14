"""Sweep many holdout PDFs to characterize junk_frac / accented_frac distribution."""
from __future__ import annotations
import json, os, re, sys, statistics
sys.path.insert(0, "/mnt/shared_storage/law-review-corpus/offprint")
from offprint.pdf_footnotes.text_extract import _load_liteparse_page_layouts

ACCENTED = re.compile(r"[áéíóúñÁÉÍÓÚÑüÜçÇßàâêîôûÀÂÊÎÔÛèÈ]")

def _is_junk(t: str) -> bool:
    if not t:
        return False
    for ch in t:
        if ch.isalnum():
            return False
    return True

def feats(pdf):
    layouts = _load_liteparse_page_layouts(pdf)
    if not layouts:
        return None
    n_pages = len(layouts)
    items_total = junk_total = accented_total = 0
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
            if _is_junk(t):
                junk_total += 1
            if ACCENTED.search(t):
                accented_total += 1
    median_sz = statistics.median(sizes) if sizes else 0.0
    return dict(
        pages=n_pages, items=items_total, median_size=median_sz,
        zero_pages=page_zero, zero_frac=page_zero/max(1,n_pages),
        junk=junk_total, junk_frac=junk_total/max(1,items_total),
        accented=accented_total, accented_frac=accented_total/max(1,items_total),
    )

data = json.load(open('artifacts/runs/holdout_1k_after_all_fixes.json'))
rows = data['rows']
# Sample: all 20 invalid + 63 empty + 50 random valid for FP estimate
import random
random.seed(0)
valid = [r for r in rows if r.get('status') == 'valid']
sampled_valid = random.sample(valid, 50)

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
        print(f"err {pdf}: {e}")
        continue
    if not f:
        continue
    f['status'] = status
    f['pdf'] = pdf
    f['doc_type'] = r.get('doc_type', '-')
    results.append(f)

# Print summary buckets
print(f"{'status':<10} {'doc_type':<22} {'pg':>4} {'items':>7} {'medsz':>6} {'zfrac':>6} {'junkfr':>7} {'accfr':>7} flag pdf")
for f in results:
    # apply detector
    pages = f['pages']
    flag = '-'
    if pages >= 3:
        if f['accented_frac'] < 0.01:
            if f['items'] == 0:
                flag = 'NO_TEXT'
            elif f['junk_frac'] >= 0.025:
                flag = 'CMAP'
            elif f['zero_frac'] >= 0.30 and f['median_size'] >= 30:
                flag = 'FONT_INFL'
    print(f"{f['status']:<10} {f['doc_type'][:22]:<22} {pages:>4} {f['items']:>7} {f['median_size']:>6.2f} {f['zero_frac']:>6.2%} {f['junk_frac']:>7.4f} {f['accented_frac']:>7.4f} {flag:>9} {os.path.basename(f['pdf'])}")

# Aggregate flag counts by status
from collections import Counter
print("\n=== Flag counts by status ===")
ct = Counter()
for f in results:
    pages = f['pages']
    flag = '-'
    if pages >= 3 and f['accented_frac'] < 0.01:
        if f['items'] == 0:
            flag = 'NO_TEXT'
        elif f['junk_frac'] >= 0.025:
            flag = 'CMAP'
        elif f['zero_frac'] >= 0.30 and f['median_size'] >= 30:
            flag = 'FONT_INFL'
    ct[(f['status'], flag)] += 1
for k, v in sorted(ct.items()):
    print(k, v)
