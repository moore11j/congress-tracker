"""Reuse only complete PDF text from identical bytes and the same page policy.

Every caller still validates its current metadata and stored evidence. No PDFs,
parsed metadata or mutable reader objects persist on disk. Retained byte/string
payloads have an 8 MiB budget, with at most 16 entries per worker process.
"""
from collections import OrderedDict
from threading import RLock
import io
import sys

_LIMIT = 8 * 1024 * 1024
_ENTRIES = 16
_cache = OrderedDict()
_lock = RLock()
_size = 0


def clear_cache():
    global _size
    with _lock:
        _cache.clear()
        _size = 0


def extract_text(raw, *, max_pages):
    global _size
    from app.clients.direct_sources import DirectSourceError
    if not raw or len(raw) > 2_000_000 or not raw.startswith(b'%PDF-'):
        raise DirectSourceError('Issuer material exceeds size limit or is empty')
    key = (raw, max_pages)
    with _lock:
        cached = _cache.get(key)
        if cached is not None:
            _cache.move_to_end(key)
            return cached[0]
    # Parsing outside the lock keeps independent documents from blocking reads.
    # Concurrent identical misses may parse twice; only one entry is retained.
    text = _extract(raw, max_pages)
    weight = sys.getsizeof(raw) + sys.getsizeof(text)
    if weight <= _LIMIT:
        with _lock:
            if key in _cache:
                _cache.move_to_end(key)
            else:
                while _cache and (_size + weight > _LIMIT or len(_cache) >= _ENTRIES):
                    _, (_, previous_weight) = _cache.popitem(last=False)
                    _size -= previous_weight
                _cache[key] = (text, weight)
                _size += weight
    return text


def _extract(raw, max_pages):
    from app.clients.direct_sources import DirectSourceError
    from pypdf import PdfReader
    from pypdf.constants import UserAccessPermissions
    try:
        reader = PdfReader(io.BytesIO(raw), strict=True)
        if reader.is_encrypted:
            # Public PDFs can use an empty user password while reserving
            # editing rights. Never try other passwords or ignore extraction
            # permissions when reading those public documents.
            permitted = int(reader.trailer['/Encrypt']['/P']) & int(UserAccessPermissions.EXTRACT)
            if not reader.decrypt('') or not permitted:
                raise DirectSourceError("Issuer PDF requires a password or prohibits extraction")
        if not 1 <= len(reader.pages) <= max_pages:
            raise DirectSourceError("Issuer PDF exceeds page limit")
        pieces, stream_bytes, text_chars = [], 0, 0
        for page in reader.pages:
            content = page.get_contents()
            if content is not None:
                stream_bytes += len(content.get_data())
            if stream_bytes > 5_000_000:
                raise DirectSourceError("Issuer PDF exceeds decoded content limit")
            piece = page.extract_text() or ""
            text_chars += len(piece)
            if text_chars > 2_000_000:
                raise DirectSourceError("Issuer PDF exceeds extracted text limit")
            pieces.append(piece)
        text = " ".join(" ".join(pieces).split())
    except DirectSourceError:
        raise
    except Exception as exc:
        raise DirectSourceError("Issuer PDF could not be parsed") from exc
    return text
