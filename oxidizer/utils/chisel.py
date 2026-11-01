"""Bounded, local documentation lookup for troubleshooting clients."""

from importlib.metadata import PackageNotFoundError, version
import hashlib
import json
from pathlib import Path, PurePosixPath
import re


class Chisel:
	MAX_DOCUMENT_BYTES = 500_000
	MAX_READ_CHARS = 12_000
	MAX_EXCERPT_CHARS = 1_000
	MAX_HEADING_CHARS = 300
	CHUNK_CHARS = 1200
	CHUNK_OVERLAP = 200
	INDEX_FORMAT = 1
	MODEL = "BAAI/bge-small-en-v1.5"
	DEFAULT_INDEX_PATH = Path("/opt/oxidizer/search")

	def __init__(self, index_path=None):
		self.index_path = Path(index_path) if index_path is not None else self.DEFAULT_INDEX_PATH
		self._table = None
		self._model = None
		self._records = None
		self._index_error = None

	def _documents(self):
		root, _ = self._root()
		for document in sorted(root.rglob("*.md")):
			relative = document.relative_to(root).as_posix()
			try:
				self._path(root, relative)
			except ValueError:
				continue
			yield relative, self._text(document)

	def _fingerprint(self):
		digest = hashlib.sha256()
		for relative, text in self._documents():
			digest.update(json.dumps([relative, text], ensure_ascii=True).encode("utf-8"))
		return digest.hexdigest()

	def _chunks(self):
		for relative, text in self._documents():
			lines, sections = self._sections(text)
			for heading, _, start, end in sections:
				content = "".join(lines[start:end])
				for offset in range(0, len(content), self.CHUNK_CHARS - self.CHUNK_OVERLAP):
					chunk = content[offset:offset + self.CHUNK_CHARS]
					if chunk.strip():
						yield {
							"id": f"{relative}:{start}:{offset}", "path": relative,
							"section": heading, "start_line": start + content[:offset].count("\n") + 1,
							"content": chunk,
						}
					if offset + self.CHUNK_CHARS >= len(content):
						break

	def _load_model(self, *, offline):
		from fastembed import TextEmbedding

		return TextEmbedding(
			model_name=self.MODEL, cache_dir=str(self.index_path / "model"),
			local_files_only=offline, threads=2,
		)

	def build_index(self):
		"""Build release assets explicitly; serving never downloads or rebuilds them."""
		import lancedb
		import pyarrow as pa

		if self.index_path.exists() and any(self.index_path.iterdir()):
			raise ValueError("Build the documentation index in a new empty directory")
		records = list(self._chunks())
		if not records:
			raise ValueError("No published documentation sections to index")
		self.index_path.mkdir(parents=True, exist_ok=True)
		model = self._load_model(offline=False)
		embeddings = list(model.passage_embed(
			[f"{record['path']}\n{record['section']}\n{record['content']}" for record in records],
			batch_size=32,
		))
		if len(embeddings) != len(records):
			raise RuntimeError("Documentation embedding count mismatch")
		dimension = len(embeddings[0])
		for record, vector in zip(records, embeddings, strict=True):
			record["vector"] = vector.tolist()
		schema = pa.schema([
			("id", pa.string()), ("path", pa.string()), ("section", pa.string()),
			("start_line", pa.int64()), ("content", pa.string()),
			("vector", pa.list_(pa.float32(), dimension)),
		])
		database = lancedb.connect(str(self.index_path / "lance"))
		database.create_table("documentation", records, schema=schema)
		_, source = self._root()
		manifest = {
			"format": self.INDEX_FORMAT, "model": self.MODEL,
			"version": self._metadata(source)["version"], "docs_sha256": self._fingerprint(),
			"fastembed_version": version("fastembed"), "lancedb_version": version("lancedb"),
			"dimension": dimension, "chunks": len(records),
		}
		(self.index_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
		return manifest

	def initialize_search(self):
		"""Validate and open bundled assets without creating an index or fetching models."""
		if self._table is not None:
			return
		try:
			manifest = json.loads((self.index_path / "manifest.json").read_text(encoding="utf-8"))
			_, source = self._root()
			if (
				manifest["format"] != self.INDEX_FORMAT or manifest["model"] != self.MODEL
				or manifest["version"] != self._metadata(source)["version"]
				or manifest["docs_sha256"] != self._fingerprint()
				or manifest["fastembed_version"] != version("fastembed")
				or manifest["lancedb_version"] != version("lancedb")
			):
				raise ValueError("Incompatible documentation index")
			if not (self.index_path / "lance" / "documentation.lance").is_dir():
				raise ValueError("Documentation table is missing")
			import lancedb

			database = lancedb.connect(str(self.index_path / "lance"))
			table = database.open_table("documentation")
			if table.count_rows() != manifest["chunks"] or manifest["chunks"] < 1:
				raise ValueError("Incomplete documentation index")
			model = self._load_model(offline=True)
			probe = next(iter(model.query_embed("documentation readiness")))
			if len(probe) != manifest["dimension"] or table.schema.field("vector").type.list_size != len(probe):
				raise ValueError("Incompatible documentation embeddings")
			records = table.to_arrow().drop(["vector"]).to_pylist()
			self._model, self._records, self._table = model, records, table
			self._index_error = None
		except Exception:
			self._index_error = {
				"code": "documentation_search_unavailable",
				"message": "Bundled documentation index/model is missing or incompatible; rebuild the API image with matching release assets.",
			}
			raise RuntimeError(f"{self._index_error['code']}: {self._index_error['message']}") from None

	def get_search_status(self):
		try:
			self.initialize_search()
		except RuntimeError:
			pass
		return {
			"ready": self._table is not None, "mode": "hybrid", "model": self.MODEL,
			"error": self._index_error,
		}

	@staticmethod
	def _root():
		package = Path(__file__).resolve().parents[1]
		bundled = package / "docs"
		if bundled.is_dir() and not bundled.is_symlink():
			return bundled, "package"
		checkout = package.parent
		source = checkout / "docs"
		if (checkout / "pyproject.toml").is_file() and source.is_dir() and not source.is_symlink():
			return source, "checkout"
		raise RuntimeError("Published documentation is unavailable")

	@staticmethod
	def _metadata(source):
		try:
			package_version = version("oxidizer-lite")
		except PackageNotFoundError:
			package_version = None
		return {"package": "oxidizer-lite", "version": package_version, "source": source}

	@staticmethod
	def _path(root, path):
		if not isinstance(path, str) or not path or len(path) > 500 or "\\" in path:
			raise ValueError("path must be a relative Markdown document path")
		relative = PurePosixPath(path)
		if relative.is_absolute() or ".." in relative.parts or relative.suffix != ".md":
			raise ValueError("path must stay inside the published documentation")
		candidate = root.joinpath(*relative.parts)
		if any(root.joinpath(*relative.parts[:index]).is_symlink() for index in range(1, len(relative.parts) + 1)):
			raise ValueError("Documentation symlinks are not allowed")
		if not candidate.resolve().is_relative_to(root.resolve()):
			raise ValueError("path must stay inside the published documentation")
		if not candidate.is_file():
			raise LookupError("Documentation not found")
		return candidate

	@classmethod
	def _text(cls, path):
		try:
			with path.open("rb") as document:
				content = document.read(cls.MAX_DOCUMENT_BYTES + 1)
			if len(content) > cls.MAX_DOCUMENT_BYTES:
				raise ValueError("Document exceeds the documentation size limit")
			return content.decode("utf-8")
		except (OSError, UnicodeError):
			raise RuntimeError("Unable to read published documentation") from None

	@classmethod
	def _sections(cls, text):
		sections = []
		title, start, level = "Introduction", 0, 0
		lines = text.splitlines(keepends=True)
		fence = None
		for index, line in enumerate(lines):
			marker = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
			if marker:
				delimiter = marker.group(1)
				if fence is None:
					fence = delimiter
				elif delimiter[0] == fence[0] and len(delimiter) >= len(fence):
					fence = None
				continue
			heading = re.match(r"^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$", line) if fence is None else None
			if heading:
				if index > start:
					sections.append((title, level, start, index))
				title, level, start = heading.group(2)[:cls.MAX_HEADING_CHARS], len(heading.group(1)), index
		if len(lines) > start:
			sections.append((title, level, start, len(lines)))
		return lines, sections

	def read_doc(self, path: str, section: str | None = None) -> dict:
		"""Read a bounded document or heading and its subsections."""
		if section is not None and (not isinstance(section, str) or not section.strip() or len(section) > 300):
			raise ValueError("section must be a non-empty heading of at most 300 characters")
		root, source = self._root()
		document = self._path(root, path)
		text = self._text(document)
		lines, sections = self._sections(text)
		start, end = 0, len(lines)
		selected = None
		if section is not None:
			matches = [entry for entry in sections if entry[0].casefold() == section.strip().casefold()]
			if not matches:
				raise LookupError("Documentation section not found")
			if len(matches) > 1:
				raise ValueError("Section heading is ambiguous; read the document instead")
			selected, level, start, _ = matches[0]
			end = next((entry[2] for entry in sections if entry[2] > start and entry[1] <= level), len(lines))
		content = "".join(lines[start:end])
		return {
			**self._metadata(source), "path": document.relative_to(root).as_posix(),
			"section": selected, "start_line": start + 1,
			"headings": [entry[0] for entry in sections][:100],
			"content": content[:self.MAX_READ_CHARS],
			"truncated": len(content) > self.MAX_READ_CHARS,
		}

	def search_docs(self, query: str, limit: int = 5) -> dict:
		"""Fuse vector and keyword ranks over release-matched documentation chunks."""
		if not isinstance(query, str) or not query.strip() or len(query) > 300:
			raise ValueError("query must contain 1 to 300 characters")
		if type(limit) is not int or not 1 <= limit <= 10:
			raise ValueError("limit must be an integer from 1 to 10")
		terms = set(re.findall(r"\w+", query.casefold()))
		if not terms:
			raise ValueError("query must contain searchable words")
		self.initialize_search()
		_, source = self._root()
		try:
			vector = next(iter(self._model.query_embed(query))).tolist()
			semantic = self._table.search(vector).distance_type("cosine").limit(50).to_list()
		except Exception:
			raise RuntimeError("documentation_search_failed: Unable to query the local documentation index") from None
		lexical = []
		for record in self._records:
			folded = record["content"].casefold()
			score = sum(term in folded for term in terms) * 10
			score += sum(term in record["section"].casefold() for term in terms) * 5
			if score:
				lexical.append((score, record))
		lexical.sort(key=lambda entry: (-entry[0], entry[1]["id"]))
		candidates = {}
		for ranking in (semantic, [entry[1] for entry in lexical[:50]]):
			for rank, record in enumerate(ranking, start=1):
				candidate = candidates.setdefault(record["id"], {**record, "score": 0.0})
				candidate["score"] += 1 / (60 + rank)
		results = []
		for record in candidates.values():
			content = record["content"]
			matches = [content.casefold().find(term) for term in terms if term in content.casefold()]
			excerpt_start = max(0, min(matches) - 150) if matches else 0
			results.append({
				"path": record["path"], "section": record["section"],
				"start_line": record["start_line"],
				"excerpt": content[excerpt_start:excerpt_start + self.MAX_EXCERPT_CHARS],
				"score": record["score"],
			})
		results.sort(key=lambda result: (-result["score"], result["path"], result["start_line"]))
		return {
			**self._metadata(source), "mode": "hybrid", "query": query,
			"results": results[:limit], "truncated": len(results) > limit,
		}


if __name__ == "__main__":
	import argparse

	parser = argparse.ArgumentParser(description="Build bundled semantic documentation assets")
	parser.add_argument("--index-path", type=Path, default=Chisel.DEFAULT_INDEX_PATH)
	arguments = parser.parse_args()
	print(json.dumps(Chisel(arguments.index_path).build_index(), sort_keys=True))
