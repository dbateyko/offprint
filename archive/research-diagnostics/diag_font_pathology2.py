"""More targeted diagnostic - per-page distribution."""
from __future__ import annotations
import os, re, sys, statistics
sys.path.insert(0, "/mnt/shared_storage/law-review-corpus/offprint")
from offprint.pdf_footnotes.text_extract import _load_liteparse_page_layouts

ACCENTED = re.compile(r"[áéíóúñÁÉÍÓÚÑüÜçÇßàâêîôûÀÂÊÎÔÛèÈ]")

def _is_junk(t: str) -> bool:
    """Glyphs that are pure punctuation/symbol Latin-1 supplement / replacement marks."""
    if not t:
        return False
    # All chars are non-letter symbols, Latin-1 supplement, or replacement
    for ch in t:
        if ch.isalnum():
            return False
    return True

def diag(pdf):
    print(f"=== {pdf} ===")
    if not os.path.exists(pdf):
        print("  MISSING")
        return
    layouts = _load_liteparse_page_layouts(pdf)
    if not layouts:
        print("  no layouts")
        return
    n_pages = len(layouts)
    accented_total = 0
    junk_total = 0
    items_total = 0
    sizes = []
    page_zero_with_raw = 0
    page_zero = 0
    for layout in layouts:
        items = list(layout.raw_items or ())
        if len(layout.lines) == 0:
            page_zero += 1
            if len(items) >= 10:
                page_zero_with_raw += 1
        for it in items:
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
    print(f"  pages={n_pages}  items={items_total}  median_size={median_sz:.2f}")
    print(f"  zero_line_pages={page_zero} ({page_zero/n_pages:.1%}) zero_with_raw_ge10={page_zero_with_raw} ({page_zero_with_raw/n_pages:.1%})")
    print(f"  junk={junk_total} ({junk_total/max(1,items_total):.4f})  accented={accented_total} ({accented_total/max(1,items_total):.4f})")

PATHS = [
    # Pathology candidates
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/illinoislawreview.org/huberfeld-5.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/illinoislawreview.org/huberfeld-8.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/illinoislawreview.org/huberfeld-14.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/mjlr.org/mcclure-2017-phd.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/bclawreview.bc.edu/63beb1f9a5219.pdf",
    # Spanish safeguards
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-3.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-8.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-10.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-11.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-14.pdf",
]
for p in PATHS:
    diag(p)
