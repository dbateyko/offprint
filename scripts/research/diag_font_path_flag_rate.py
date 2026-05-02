"""Compute font-pathology flag rate on holdout 1K, by status."""
from __future__ import annotations
import json, os, sys
from collections import Counter
sys.path.insert(0, "/mnt/shared_storage/law-review-corpus/offprint")
from offprint.pdf_footnotes.text_extract import (
    _load_liteparse_page_layouts,
    _detect_liteparse_font_pathology,
)

data = json.load(open('artifacts/runs/holdout_1k_after_font_path.json'))
rows = data['rows']

flag_ct = Counter()
flag_examples = {'empty': [], 'invalid': [], 'valid': [], 'valid_with_gaps': []}
for r in rows:
    pdf = r.get('pdf')
    if not pdf or not os.path.exists(pdf):
        continue
    try:
        layouts = _load_liteparse_page_layouts(pdf)
    except Exception:
        continue
    if not layouts:
        continue
    flag = _detect_liteparse_font_pathology(layouts)
    status = r.get('status', '?')
    if flag:
        flag_ct[(status, 'FLAG')] += 1
        if len(flag_examples.get(status, [])) < 5:
            flag_examples.setdefault(status, []).append(os.path.basename(pdf))
    else:
        flag_ct[(status, '-')] += 1

print('=== Flag distribution (status × flag) ===')
for k, v in sorted(flag_ct.items()):
    print(f'  {k}: {v}')

flagged_total = sum(v for (_, fl), v in flag_ct.items() if fl == 'FLAG')
total = sum(flag_ct.values())
print(f'\nTotal flagged: {flagged_total}/{total} = {flagged_total/total:.1%}')

valid_total = sum(v for (s, _), v in flag_ct.items() if s in ('valid', 'valid_with_gaps'))
valid_flagged = sum(v for (s, fl), v in flag_ct.items() if s in ('valid','valid_with_gaps') and fl == 'FLAG')
empty_invalid_total = sum(v for (s, _), v in flag_ct.items() if s in ('empty', 'invalid'))
empty_invalid_flagged = sum(v for (s, fl), v in flag_ct.items() if s in ('empty', 'invalid') and fl == 'FLAG')
print(f'\nFP rate (valid flagged / valid total): {valid_flagged}/{valid_total} = {valid_flagged/max(1,valid_total):.2%}')
print(f'TP rate (empty+invalid flagged / total): {empty_invalid_flagged}/{empty_invalid_total} = {empty_invalid_flagged/max(1,empty_invalid_total):.2%}')

print('\n=== Examples ===')
for status, exs in flag_examples.items():
    print(f'  {status}: {exs}')
