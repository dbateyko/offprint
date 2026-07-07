"""Look closely at FP cases."""
from __future__ import annotations
import json, os, sys
sys.path.insert(0, "/mnt/shared_storage/law-review-corpus/offprint")
from offprint.pdf_footnotes.text_extract import _load_liteparse_page_layouts, _detect_liteparse_font_pathology
import re
ACCENTED = re.compile(r"[áéíóúñÁÉÍÓÚÑüÜçÇßàâêîôûÀÂÊÎÔÛèÈùÙ]")
CMAP = frozenset("!#$%*+")

def feats(pdf):
    layouts = _load_liteparse_page_layouts(pdf)
    if not layouts:
        return None
    items = sum(len(l.raw_items or ()) for l in layouts)
    cmap = 0
    acc = 0
    ctrl = 0
    for layout in layouts:
        for it in layout.raw_items or ():
            t = str(it.get('text') or '')
            if not t:
                continue
            if len(t) == 1 and t in CMAP:
                cmap += 1
            if ACCENTED.search(t):
                acc += 1
    return dict(pages=len(layouts), items=items, cmap_frac=cmap/max(1,items), acc_frac=acc/max(1,items))

data = json.load(open('artifacts/runs/holdout_1k_after_font_path.json'))
rows = data['rows']
fps = []
for r in rows:
    if r.get('status') not in ('valid', 'valid_with_gaps'):
        continue
    pdf = r.get('pdf')
    if not pdf or not os.path.exists(pdf):
        continue
    layouts = _load_liteparse_page_layouts(pdf)
    if not layouts:
        continue
    if _detect_liteparse_font_pathology(layouts):
        f = feats(pdf)
        fps.append((r, f))
print(f'{len(fps)} FPs')
for r, f in fps:
    print(f"  pg={f['pages']} items={f['items']} cmap={f['cmap_frac']:.4f} acc={f['acc_frac']:.4f} doc_type={r.get('doc_type','-')} status={r.get('status','-')} notes={r.get('notes',0)} {os.path.basename(r['pdf'])}")
