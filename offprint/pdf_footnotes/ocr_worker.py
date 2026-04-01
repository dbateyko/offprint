from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, Sequence

from pypdf import PdfReader, PdfWriter

from .text_extract import ExtractedDocument, ExtractedLine, ExtractedPage


def _split_lines(text: str, page_number: int) -> list[ExtractedLine]:
    lines: list[ExtractedLine] = []
    for raw in (text or "").splitlines():
        cleaned = " ".join(raw.split())
        if cleaned:
            lines.append(ExtractedLine(text=cleaned, page_number=page_number, source="ocr"))
    return lines


class OCRWorkerPool:
    def __init__(self, workers: int = 2, dpi: int = 250, backend: str = "olmocr"):
        self.workers = max(1, int(workers))
        self.dpi = max(72, int(dpi))
        self.backend = (backend or "olmocr").strip().lower()
        if self.backend != "olmocr":
            raise ValueError(
                f"Unsupported OCR backend '{backend}'. This pipeline supports only 'olmocr'."
            )

    def close(self) -> None:
        pass

    def available(self) -> bool:
        try:
            return importlib.util.find_spec("olmocr.pipeline") is not None
        except ModuleNotFoundError:
            return False

    def _olmocr_command(self, workspace: Path, pdf_paths: Sequence[str]) -> list[str]:
        cmd = [
            sys.executable,
            "-m",
            "olmocr.pipeline",
            str(workspace),
            "--markdown",
            "--pdfs",
        ]
        cmd.extend(str(path) for path in pdf_paths)
        # Optional runtime tuning hooks for operators.
        server_url = os.getenv("OLMOCR_SERVER_URL", "").strip()
        if server_url:
            cmd.extend(["--server", server_url])
        model_name = os.getenv("OLMOCR_MODEL", "").strip()
        if model_name:
            cmd.extend(["--model", model_name])
        tensor_parallel = os.getenv("OLMOCR_TENSOR_PARALLEL_SIZE", "").strip()
        if tensor_parallel:
            cmd.extend(["--tensor-parallel-size", tensor_parallel])
        data_parallel = os.getenv("OLMOCR_DATA_PARALLEL_SIZE", "").strip()
        if data_parallel:
            cmd.extend(["--data-parallel-size", data_parallel])
        gpu_mem = os.getenv("OLMOCR_GPU_MEMORY_UTILIZATION", "").strip()
        if gpu_mem:
            cmd.extend(["--gpu-memory-utilization", gpu_mem])
        max_model_len = os.getenv("OLMOCR_MAX_MODEL_LEN", "").strip()
        if max_model_len:
            cmd.extend(["--max_model_len", max_model_len])
        max_concurrent = os.getenv("OLMOCR_MAX_CONCURRENT_REQUESTS", "").strip()
        if max_concurrent:
            cmd.extend(["--max_concurrent_requests", max_concurrent])
        local_workers = os.getenv("OLMOCR_WORKERS", "").strip()
        if local_workers:
            cmd.extend(["--workers", local_workers])
        return cmd

    def _run_olmocr_markdown_batch(
        self, pdf_paths: Sequence[str]
    ) -> tuple[dict[str, str], list[str]]:
        warnings: list[str] = []
        timeout_seconds = int(os.getenv("OLMOCR_TIMEOUT_SECONDS", "900"))
        if not pdf_paths:
            return {}, warnings
        with TemporaryDirectory(prefix="olmocr_workspace_") as tmp_dir:
            workspace = Path(tmp_dir) / "workspace"
            workspace.mkdir(parents=True, exist_ok=True)
            cmd = self._olmocr_command(workspace, pdf_paths)
            try:
                proc = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                    timeout=max(60, timeout_seconds),
                )
            except subprocess.TimeoutExpired:
                warnings.append(
                    f"olmocr timed out after {max(60, timeout_seconds)}s for {len(pdf_paths)} input pdf(s)"
                )
                return {}, warnings
            if proc.returncode != 0:
                stderr_tail = (proc.stderr or "").strip().splitlines()[-5:]
                joined_tail = " | ".join(stderr_tail) if stderr_tail else "no stderr"
                warnings.append(f"olmocr failed with rc={proc.returncode}: {joined_tail}")
                return {}, warnings

            markdown_root = workspace / "markdown"
            md_candidates = sorted(markdown_root.rglob("*.md"))
            if not md_candidates:
                warnings.append("olmocr produced no markdown output")
                return {}, warnings

            markdown_by_path: dict[str, str] = {}
            for input_pdf in pdf_paths:
                input_stem = Path(input_pdf).stem.lower()
                chosen: Path | None = None
                for candidate in md_candidates:
                    if candidate.stem.lower() == input_stem:
                        chosen = candidate
                        break
                if chosen is None:
                    continue
                markdown_by_path[str(input_pdf)] = chosen.read_text(
                    encoding="utf-8", errors="ignore"
                )
            return markdown_by_path, warnings

    def _run_olmocr_markdown(self, pdf_path: str) -> tuple[str | None, list[str]]:
        markdown_by_path, warnings = self._run_olmocr_markdown_batch([pdf_path])
        markdown = markdown_by_path.get(str(pdf_path))
        if markdown is None and not warnings:
            warnings.append("olmocr markdown missing for input pdf")
        return markdown, warnings

    def extract_document(
        self,
        pdf_path: str,
        page_numbers: Sequence[int] | None = None,
    ) -> tuple[ExtractedDocument | None, list[str]]:
        warnings: list[str] = []
        if not self.available():
            warnings.append("olmocr module unavailable; install olmocr to enable OCR")
            return None, warnings

        if page_numbers:
            try:
                reader = PdfReader(pdf_path)
            except Exception as exc:
                warnings.append(f"olmocr page-filter preflight failed: {exc}")
                return None, warnings

            total_pages = len(reader.pages)
            requested_pages = sorted(
                {
                    int(page)
                    for page in page_numbers
                    if int(page) > 0 and int(page) <= max(0, total_pages)
                }
            )
            if not requested_pages:
                warnings.append("olmocr page filter resolved to no valid pages")
                return None, warnings

            extracted_pages: list[ExtractedPage] = []
            with TemporaryDirectory(prefix="olmocr_pages_batch_") as page_tmp_dir:
                page_tmp_root = Path(page_tmp_dir)
                per_page_pdf_paths: list[str] = []
                page_num_by_pdf_path: dict[str, int] = {}
                for page_num in requested_pages:
                    one_page_pdf = page_tmp_root / f"page_{page_num}.pdf"
                    writer = PdfWriter()
                    writer.add_page(reader.pages[page_num - 1])
                    with one_page_pdf.open("wb") as handle:
                        writer.write(handle)
                    one_page_pdf_str = str(one_page_pdf)
                    per_page_pdf_paths.append(one_page_pdf_str)
                    page_num_by_pdf_path[one_page_pdf_str] = page_num

                raw_batch_size = os.getenv("OLMOCR_PAGE_BATCH_SIZE", "").strip()
                try:
                    batch_size = max(1, int(raw_batch_size)) if raw_batch_size else 4
                except Exception:
                    batch_size = 4
                markdown_by_path: dict[str, str] = {}
                for idx in range(0, len(per_page_pdf_paths), batch_size):
                    chunk_paths = per_page_pdf_paths[idx : idx + batch_size]
                    chunk_markdown, chunk_warnings = self._run_olmocr_markdown_batch(chunk_paths)
                    warnings.extend(chunk_warnings)
                    markdown_by_path.update(chunk_markdown)
                    timed_out_chunk = any("timed out" in str(w).lower() for w in chunk_warnings)
                    if timed_out_chunk and len(chunk_paths) > 1:
                        warnings.append(
                            f"olmocr chunk timeout; retrying {len(chunk_paths)} pages individually"
                        )
                        for single_path in chunk_paths:
                            if single_path in markdown_by_path:
                                continue
                            single_markdown, single_warnings = self._run_olmocr_markdown(single_path)
                            warnings.extend(single_warnings)
                            if single_markdown is not None:
                                markdown_by_path[single_path] = single_markdown

                for one_page_pdf_path in per_page_pdf_paths:
                    page_num = page_num_by_pdf_path[one_page_pdf_path]
                    markdown = markdown_by_path.get(one_page_pdf_path)
                    if markdown is None:
                        warnings.append(f"page {page_num}: olmocr markdown missing")
                        continue
                    lines = _split_lines(markdown, page_number=page_num)
                    extracted_pages.append(
                        ExtractedPage(
                            page_number=page_num,
                            body_lines=lines,
                            note_lines=lines,
                            raw_text=markdown,
                            source="ocr",
                        )
                    )
            if not extracted_pages:
                warnings.append("olmocr produced no markdown output for requested pages")
                return None, warnings

            extracted_pages.sort(key=lambda page: page.page_number)
            return (
                ExtractedDocument(
                    pdf_path=pdf_path,
                    pages=extracted_pages,
                    warnings=warnings,
                    parser="olmocr",
                ),
                warnings,
            )

        markdown, ocr_warnings = self._run_olmocr_markdown(pdf_path)
        warnings.extend(ocr_warnings)
        if markdown is None:
            return None, warnings
        lines = _split_lines(markdown, page_number=1)
        page = ExtractedPage(
            page_number=1,
            body_lines=lines,
            note_lines=lines,
            raw_text=markdown,
            source="ocr",
        )
        return (
            ExtractedDocument(pdf_path=pdf_path, pages=[page], warnings=warnings, parser="olmocr"),
            warnings,
        )


def iter_all_pages(page_count: int) -> Iterable[int]:
    yield from range(1, max(0, page_count) + 1)
