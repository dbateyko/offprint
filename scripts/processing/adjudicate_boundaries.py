#!/usr/bin/env python3
"""Adjudicate weak boundaries against a local open-weight model, then score.

Reads the blind queue from `build_adjudication_queue.py`, asks an
OpenAI-compatible endpoint (local vLLM) which candidate page opens each piece,
and writes one answer per item. A second pass scores the answers against what
the solver chose and reports which `review` documents are now fully confirmed.

The model never sees the solver's choice -- see `build_adjudication_queue.py`.
Agreement measured under anchoring is not evidence, and this project has already
been bitten by that once (`ISSUE_SPLITTER_HANDOFF_2026-08-07.md` §7).

Conventions follow `offprint-data-ops/labeling/annotate_gold_27b.py`: an
`OpenAI` client against `localhost:8000/v1`, `response_format` json_schema (vLLM
ignores the legacy `guided_json`), temperature 0, thinking disabled. Resumable --
items already answered are skipped.

    # run (needs vLLM up; see docs/skills/run-vllm-qwen35-27b/SKILL.md)
    python scripts/processing/adjudicate_boundaries.py \
        --queue queue.jsonl --out answers.jsonl --model <served-model-name>

    # score without calling the model
    python scripts/processing/adjudicate_boundaries.py \
        --queue queue.jsonl --out answers.jsonl --score-only
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.processing.build_adjudication_queue import render_prompt  # noqa: E402

DEFAULT_BASE_URL = "http://localhost:8000/v1"

SYSTEM = (
    "You identify where articles begin in scanned law-journal issues. "
    "You answer only with the physical page number of the page that opens the "
    "named piece, or 0 if none of the offered pages opens it. Return JSON only."
)


def schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "physical_page": {
                "type": "integer",
                "description": "physical page that opens the piece, or 0 for none of these",
            },
            "evidence": {
                "type": "string",
                "description": "the display title / byline line you saw, or why none qualifies",
            },
        },
        "required": ["physical_page", "evidence"],
        "additionalProperties": False,
    }


def _strip_think(content: str) -> str:
    if "<think>" in content:
        content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
    start, end = content.find("{"), content.rfind("}") + 1
    return content[start:end] if start >= 0 and end > start else content


def ask(client, model: str, item: dict, max_tokens: int) -> dict:
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": render_prompt(item)},
        ],
        temperature=0.0,
        max_tokens=max_tokens,
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "boundary_adjudication", "schema": schema()},
        },
        extra_body={"chat_template_kwargs": {"enable_thinking": False}},
    )
    return json.loads(_strip_think(response.choices[0].message.content or ""))


def score(queue: list[dict], answers: dict[str, dict]) -> dict:
    """Compare blind answers to the solver, and report per-document outcomes."""
    agree = disagree = none_of_these = unanswered = 0
    per_document: dict[str, list[bool]] = defaultdict(list)

    for item in queue:
        document = item["pdf_relpath"] or item["pdf_path"]
        answer = answers.get(item["item_id"])
        if answer is None:
            unanswered += 1
            per_document[document].append(False)
            continue
        chosen = answer.get("physical_page", 0)
        solver = item["_solver"]["physical_page"]
        if chosen == 0:
            none_of_these += 1
            per_document[document].append(False)
        elif chosen == solver:
            agree += 1
            per_document[document].append(True)
        else:
            disagree += 1
            per_document[document].append(False)

    answered = agree + disagree + none_of_these
    confirmed = [d for d, flags in per_document.items() if all(flags)]
    return {
        "items": len(queue),
        "answered": answered,
        "unanswered": unanswered,
        "agree_with_solver": agree,
        "disagree": disagree,
        "none_of_these": none_of_these,
        "agreement_rate": round(agree / answered, 4) if answered else None,
        "documents_with_weak_boundaries": len(per_document),
        "documents_fully_confirmed": len(confirmed),
        "confirmed_documents": sorted(confirmed),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--model", default="")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--score-only", action="store_true")
    parser.add_argument("--report", default="")
    options = parser.parse_args()

    queue = [json.loads(line) for line in Path(options.queue).read_text().splitlines() if line.strip()]
    if options.limit:
        queue = queue[: options.limit]

    out_path = Path(options.out)
    answers: dict[str, dict] = {}
    if out_path.exists():
        for line in out_path.read_text().splitlines():
            if line.strip():
                row = json.loads(line)
                answers[row["item_id"]] = row["answer"]

    if not options.score_only:
        if not options.model:
            parser.error("--model is required unless --score-only")
        from openai import OpenAI

        client = OpenAI(base_url=options.base_url, api_key="unused")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("a", encoding="utf-8") as handle:
            for index, item in enumerate(queue, start=1):
                if item["item_id"] in answers:
                    continue
                try:
                    answer = ask(client, options.model, item, options.max_tokens)
                except Exception as error:
                    print(f"[{index}] {item['item_id']}: {type(error).__name__}: {error}", file=sys.stderr)
                    continue
                answers[item["item_id"]] = answer
                handle.write(
                    json.dumps({"item_id": item["item_id"], "answer": answer}, ensure_ascii=False) + "\n"
                )
                handle.flush()
                if index % 25 == 0:
                    print(f"  {index}/{len(queue)}", file=sys.stderr)

    report = score(queue, answers)
    text = json.dumps(report, indent=2)
    if options.report:
        Path(options.report).write_text(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
