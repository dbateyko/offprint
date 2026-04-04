import pytest
import os
import sys

# Ensure offprint is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture
def sample_pdf_path(tmp_path):
    """Fixture to provide a dummy PDF path for testing extractors."""
    p = tmp_path / "dummy.pdf"
    p.write_bytes(b"%PDF-1.4\n%EOF\n")
    return str(p)
