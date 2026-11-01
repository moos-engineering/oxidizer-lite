import hashlib
import re

import pytest


@pytest.fixture
def index_documentation(monkeypatch, tmp_path):
    import numpy as np
    from oxidizer.utils.chisel import Chisel

    class EmbeddingModel:
        def passage_embed(self, texts, **kwargs):
            for text in texts:
                vector = np.zeros(32, dtype=np.float32)
                for term in re.findall(r"\w+", text.casefold()):
                    vector[int(hashlib.sha256(term.encode()).hexdigest()[:8], 16) % 32] += 1
                yield vector / max(np.linalg.norm(vector), 1)

        query_embed = passage_embed

    monkeypatch.setattr(Chisel, "_load_model", lambda self, *, offline: EmbeddingModel())
    count = 0

    def build(service):
        nonlocal count
        count += 1
        service.index_path = tmp_path / f"index-{count}"
        service._table = service._model = service._records = None
        service.build_index()
        return service

    return build