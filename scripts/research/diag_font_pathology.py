"""Diagnose liteparse layout characteristics for suspect PDFs."""
from __future__ import annotations
import os, re, sys, statistics
sys.path.insert(0, "/mnt/shared_storage/law-review-corpus/offprint")
from offprint.pdf_footnotes.text_extract import _load_liteparse_page_layouts

ACCENTED = re.compile(r"[áéíóúñÁÉÍÓÚÑüÜçÇßàâêîôûÀÂÊÎÔÛèÈ]")
JUNK = re.compile(r"^[ -ÿ�\-]+$")  # Latin-1 supplement / replacement chars

def diag(pdf):
    print(f"=== {pdf} ===")
    if not os.path.exists(pdf):
        print("  MISSING")
        return
    try:
        layouts = _load_liteparse_page_layouts(pdf)
    except Exception as e:
        print(f"  load error: {e}")
        return
    if not layouts:
        print("  no layouts")
        return
    n_pages = len(layouts)
    zero_line_pages = 0
    zero_lines_with_raw = 0
    all_sizes = []
    junk_glyph_count = 0
    accented_count = 0
    total_items = 0
    sample_texts = []
    for layout in layouts:
        items = list(layout.raw_items or ())
        if len(layout.lines) == 0:
            zero_line_pages += 1
            if len(items) >= 10:
                zero_lines_with_raw += 1
        for it in items:
            t = str(it.get("text") or "")
            sz = float(it.get("fontSize") or 0.0)
            if sz > 0:
                all_sizes.append(sz)
            total_items += 1
            if JUNK.match(t):
                junk_glyph_count += 1
            if ACCENTED.search(t):
                accented_count += 1
        if len(sample_texts) < 5 and items:
            sample_texts.append([str(it.get("text") or "")[:30] for it in items[:6]])
    median_sz = statistics.median(all_sizes) if all_sizes else 0.0
    print(f"  pages={n_pages}  zero_line_pages={zero_line_pages}  zero_with_raw_ge10={zero_lines_with_raw}")
    print(f"  total_items={total_items}  median_font_size={median_sz:.2f}")
    junk_frac = junk_glyph_count / max(1, total_items)
    accented_frac = accented_count / max(1, total_items)
    print(f"  junk_glyph_frac={junk_frac:.4f}  accented_frac={accented_frac:.4f}")
    print(f"  zero_pages_frac={zero_line_pages/n_pages:.3f}  zero_with_raw_frac={zero_lines_with_raw/n_pages:.3f}")
    for s in sample_texts[:3]:
        print(f"    sample: {s}")

PATHS = [
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/illinoislawreview.org/huberfeld-5.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/illinoislawreview.org/huberfeld-8.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/illinoislawreview.org/huberfeld-14.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/illinoislawreview.org/huberfeld-10.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/bclawreview.bc.edu/63beb1f9a5219.pdf",
    # MJLR Alegreya
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/mjlr.org/mcclure-2017-phd.pdf",
    # Spanish-language safeguard test
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-3.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-8.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-14.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-10.pdf",
    "/mnt/shared_storage/law-review-corpus/corpus/scraped/derecho.uprrp.edu/tabla-de-citaci-n-11.pdf",
]

for p in PATHS:
    diag(p)
