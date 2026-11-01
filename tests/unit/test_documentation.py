from pathlib import Path

import pytest

from oxidizer.utils.chisel import Chisel


@pytest.fixture
def docs(tmp_path, monkeypatch, index_documentation):
    root = tmp_path / "docs"
    root.mkdir()
    (root / "guide.md").write_text(
        "# Operator Guide\n\nOverview.\n\n## Pending Work\n\nRedis pending entries need review.\n"
        "### Retry\n\nAppend can duplicate on retry.\n\n## Logs\n\nInspect logs.\n"
        "```python\n# Not A Heading\n```\n"
    )
    (tmp_path / "PRODUCTION.md").write_text("private release notes")
    service = Chisel()
    monkeypatch.setattr(service, "_root", lambda: (root, "test"))
    return index_documentation(service)


def test_read_doc_and_section_include_subsections_not_siblings(docs):
    result = docs.read_doc("guide.md", "pending work")
    assert result["section"] == "Pending Work"
    assert "### Retry" in result["content"]
    assert "## Logs" not in result["content"]
    assert result["path"] == "guide.md"
    assert result["start_line"] == 5
    assert result["truncated"] is False
    assert "Not A Heading" not in result["headings"]
    assert "# Operator Guide" in docs.read_doc("guide.md")["content"]


def test_search_docs_returns_bounded_citable_results(docs):
    result = docs.search_docs("Redis pending", limit=1)
    assert result["results"][0]["path"] == "guide.md"
    assert result["results"][0]["section"] == "Pending Work"
    assert "Redis pending" in result["results"][0]["excerpt"]
    assert all(len(entry["excerpt"]) <= docs.MAX_EXCERPT_CHARS for entry in result["results"])
    assert result["mode"] == "hybrid"
    assert all(entry["path"] == "guide.md" for entry in docs.search_docs("private release notes")["results"])
    assert docs.search_docs("unfindableword")["results"]


def test_search_reports_unavailable_index_without_keyword_fallback(docs, tmp_path):
    docs.index_path = tmp_path / "absent-index"
    docs._table = None
    with pytest.raises(RuntimeError, match="documentation_search_unavailable"):
        docs.search_docs("Redis pending")
    status = docs.get_search_status()
    assert status["ready"] is False
    assert status["mode"] == "hybrid"
    assert status["error"]["code"] == "documentation_search_unavailable"


@pytest.mark.parametrize("path", ["../PRODUCTION.md", "/etc/passwd", "..\\PRODUCTION.md", "guide.txt", ""])
def test_read_doc_rejects_paths_outside_markdown_catalog(docs, path):
    with pytest.raises(ValueError):
        docs.read_doc(path)


def test_symlinks_are_excluded_from_reads_and_search(docs, tmp_path):
    root, _ = docs._root()
    (root / "secret.md").symlink_to(tmp_path / "PRODUCTION.md")
    with pytest.raises(ValueError, match="symlinks"):
        docs.read_doc("secret.md")
    assert all(entry["path"] != "secret.md" for entry in docs.search_docs("private")["results"])


def test_missing_sections_size_limits_and_truncation(docs):
    with pytest.raises(LookupError):
        docs.read_doc("missing.md")
    with pytest.raises(LookupError):
        docs.read_doc("guide.md", "missing heading")
    root, _ = docs._root()
    (root / "long.md").write_text("# Long\n" + "word " * 4000)
    result = docs.read_doc("long.md")
    assert result["truncated"] is True
    assert len(result["content"]) == docs.MAX_READ_CHARS
    (root / "large.md").write_bytes(b"a" * (docs.MAX_DOCUMENT_BYTES + 1))
    with pytest.raises(ValueError, match="size limit"):
        docs.read_doc("large.md")


@pytest.mark.parametrize("query,limit", [("", 5), ("...", 5), ("x" * 301, 5), ("redis", 0), ("redis", 11), ("redis", True)])
def test_search_validation(docs, query, limit):
    with pytest.raises(ValueError):
        docs.search_docs(query, limit)


def test_headings_are_bounded_and_duplicate_sections_are_explicit(docs, index_documentation):
    root, _ = docs._root()
    (root / "headings.md").write_text("# " + "redis " * 1000 + "\n\n## Same\nfirst\n## Same\nsecond\n")
    index_documentation(docs)
    result = docs.read_doc("headings.md")
    assert all(len(heading) <= docs.MAX_HEADING_CHARS for heading in result["headings"])
    assert len(docs.search_docs("redis")["results"][0]["section"]) <= docs.MAX_HEADING_CHARS
    with pytest.raises(ValueError, match="ambiguous"):
        docs.read_doc("headings.md", "Same")


def test_documentation_prefers_bundled_docs_and_uses_checkout_only_as_fallback(tmp_path, monkeypatch):
    import oxidizer.utils.chisel as module

    package = tmp_path / "oxidizer"
    (package / "utils").mkdir(parents=True)
    (tmp_path / "docs").mkdir()
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'oxidizer-lite'\n")
    monkeypatch.setattr(module, "__file__", str(package / "utils/chisel.py"))
    assert Chisel._root() == (tmp_path / "docs", "checkout")
    (package / "docs").mkdir()
    assert Chisel._root() == (package / "docs", "package")
    (package / "docs").rmdir()
    (tmp_path / "pyproject.toml").unlink()
    with pytest.raises(RuntimeError, match="unavailable"):
        Chisel._root()


@pytest.mark.parametrize("field,value", [
    ("format", -1), ("model", "other-model"), ("version", "other-release"),
    ("docs_sha256", "stale"), ("dimension", 999), ("chunks", 999),
    ("fastembed_version", "other-version"), ("lancedb_version", "other-version"),
])
def test_incompatible_index_is_unavailable(docs, field, value):
    import json

    path = docs.index_path / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest[field] = value
    path.write_text(json.dumps(manifest))
    assert docs.get_search_status()["ready"] is False
    with pytest.raises(RuntimeError, match="documentation_search_unavailable"):
        docs.search_docs("pending")


def test_changed_documents_require_rebuild_but_reads_still_work(docs):
    root, _ = docs._root()
    (root / "guide.md").write_text("# Changed\nUpdated documentation\n")
    assert docs.get_search_status()["ready"] is False
    assert "Updated documentation" in docs.read_doc("guide.md")["content"]


def test_missing_lance_storage_is_not_created_during_readiness(docs, tmp_path):
    database = docs.index_path / "lance"
    database.rename(tmp_path / "removed-lance")
    assert docs.get_search_status()["ready"] is False
    assert not database.exists()


def test_opening_index_uses_offline_model_and_does_not_rebuild(docs, monkeypatch):
    load = docs._load_model
    calls = []

    def offline_model(*, offline):
        calls.append(offline)
        return load(offline=offline)

    monkeypatch.setattr(docs, "_load_model", offline_model)
    monkeypatch.setattr(docs, "build_index", lambda: pytest.fail("Serving must not build an index"))
    assert docs.get_search_status()["ready"] is True
    docs.search_docs("pending")
    assert calls == [True]


def test_chunks_are_bounded_and_preserve_line_references(docs):
    root, _ = docs._root()
    (root / "long.md").write_text("# Long\n" + "payload line\n" * 400)
    chunks = [chunk for chunk in docs._chunks() if chunk["path"] == "long.md"]
    assert len(chunks) > 1
    assert all(len(chunk["content"]) <= docs.CHUNK_CHARS for chunk in chunks)
    assert chunks[0]["start_line"] == 1
    assert chunks[1]["start_line"] > 1