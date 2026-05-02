"""Look at actual character content of suspect 'junk' tokens."""
from __future__ import annotations
import sys, os
from collections import Counter
sys.path.insert(0, "/mnt/shared_storage/law-review-corpus/offprint")
from offprint.pdf_footnotes.text_extract import _load_liteparse_page_layouts

def chars_for(pdf, label):
    print(f"=== {label}: {pdf} ===")
    layouts = _load_liteparse_page_layouts(pdf)
    if not layouts:
        return
    # Count chars in tokens with no alphanumeric
    cc = Counter()
    n_punct_tokens = 0
    n_total = 0
    for layout in layouts:
        for it in layout.raw_items or ():
            t = str(it.get("text") or "")
            n_total += 1
            if t and not any(ch.isalnum() for ch in t):
                n_punct_tokens += 1
                for ch in t:
                    cc[ch] += 1
    print(f"  total_tokens={n_total} punct_only_tokens={n_punct_tokens}")
    top = cc.most_common(20)
    for ch, n in top:
        print(f"    {repr(ch)} U+{ord(ch):04X}  {n}")

# A valid one with high junk_frac
chars_for("/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-3.pdf", "Spanish (good)")
chars_for("/mnt/shared_storage/law-review-corpus/corpus/scraped/mjlr.org/mcclure-2017-phd.pdf", "MJLR (pathology)")
# Try a "valid" CMAP-flagged one
import json
data = json.load(open('artifacts/runs/holdout_1k_after_all_fixes.json'))
rows = data['rows']
# Pick one that was flagged CMAP but valid: hill-4511.pdf
chars_for("/mnt/shared_storage/law-review-corpus/corpus/scraped/lawreview.richmond.edu/hill-4511.pdf",
    "valid CMAP-flagged hill-4511")
chars_for("/mnt/shared_storage/law-review-corpus/corpus/scraped/bclawreview.bc.edu/63beb1f9a5219.pdf", "bclawreview (invalid)")
