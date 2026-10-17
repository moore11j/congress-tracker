"""Pin filing bytes while retaining SEC's variable terminal delivery-script path."""
import hashlib
import re

POLICY = 'sec_terminal_delivery_script_path_v1'
_TERMINAL = re.compile(
    rb'<script type="text/javascript"  src="(?P<path>/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+){2,15})"></script>'
    rb'(?=</(?:body|BODY)>\s*</(?:html|HTML)>(?:\s*</TEXT>\s*</DOCUMENT>)?\s*\Z)'
)


def document_digest(raw):
    match = _TERMINAL.search(raw)
    if match is None or len(match['path']) > 256:
        raise ValueError('sec_delivery_script_shape')
    start, end = match.span('path')
    digest = hashlib.sha256()
    digest.update(memoryview(raw)[:start])
    digest.update(b'__SEC_DELIVERY_PATH__')
    digest.update(memoryview(raw)[end:])
    return digest.hexdigest()


def verified_original(raw, source):
    """Never normalize financial text, tags, whitespace, attributes or units."""
    if not isinstance(raw, bytes) or not raw:
        raise ValueError('sec_original_type_or_empty')
    received = hashlib.sha256(raw).hexdigest()
    if received != source['sha256']:
        if (source.get('document_integrity_policy') != POLICY
                or document_digest(raw) != source.get('document_sha256')):
            raise ValueError('sec_original_changed')
    return {'source_sha256': received, 'reviewed_source_sha256': source['sha256'],
            'document_sha256': source.get('document_sha256'),
            'document_integrity_policy': source.get('document_integrity_policy')}
