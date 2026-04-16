from __future__ import annotations

import hashlib
import math


DUMMY_EMBEDDING_MODEL_ID = "dummy:v0"
DUMMY_EMBEDDING_DIM = 64


def embed_text(text: str, *, dim: int = DUMMY_EMBEDDING_DIM) -> list[float]:
    """
    Deterministic dummy embedder.

    Goals:
    - No external dependencies.
    - Stable output across runs and machines.
    - Some robustness to small input variants (character 3-gram hashing).

    This is NOT intended to be a high-quality semantic embedding.
    """
    if dim <= 0:
        raise ValueError("dim must be > 0")

    normed = _normalize(text)
    grams = _char_ngrams(normed, n=3)
    if not grams and normed:
        grams = [normed]

    vec = [0.0] * dim
    for gram in grams:
        digest = hashlib.sha256(gram.encode("utf-8")).digest()
        idx = int.from_bytes(digest[0:4], "little") % dim
        sign = 1.0 if (digest[4] & 1) == 1 else -1.0
        vec[idx] += sign

    # L2 normalize
    l2 = math.sqrt(sum(v * v for v in vec))
    if l2 > 0:
        vec = [v / l2 for v in vec]
    return vec


def _normalize(text: str) -> str:
    # Keep letters/digits/CJK, drop whitespace and most punctuation to reduce trivial variance.
    out = []
    for ch in (text or ""):
        if ch.isspace():
            continue
        out.append(ch.lower())
    return "".join(out)


def _char_ngrams(text: str, *, n: int) -> list[str]:
    if n <= 0:
        return []
    if not text or len(text) < n:
        return []
    return [text[i : i + n] for i in range(0, len(text) - n + 1)]

