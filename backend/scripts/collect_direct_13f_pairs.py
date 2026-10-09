"""Bounded SEC prior/current-quarter source capture, without canonical writes."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--staging', type=Path, required=True)
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--ciks', nargs='+', default=['0001427999', '0000883597', '0001844830', '0001788558'])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2] / 'artifacts' / 'direct-feeds'
    output = args.output.resolve()
    if not output.is_relative_to(root.resolve()):
        parser.error('Output must remain under artifacts/direct-feeds')
    output.mkdir(parents=True, exist_ok=True)
    from app.clients.direct_sources import DirectSourceClient
    from app.services.direct_feed_collection import parse_document
    client = DirectSourceClient()
    if not 1 <= len(args.ciks) <= 10 or any(not c.isdigit() or len(c) > 10 for c in args.ciks):
        parser.error('Provide one to ten numeric CIKs')
    cik_list = list(dict.fromkeys(c.zfill(10) for c in args.ciks))
    with sqlite3.connect(args.staging.resolve().as_uri() + '?mode=ro', uri=True) as db:
        saved = {key: digest for key, digest in db.execute(
            "SELECT source_key, content_hash FROM direct_feed_documents WHERE feed='sec_13f'")}
    documents = []
    for cik in cik_list:
        history = output / (cik + '-submissions.json')
        if not history.exists():
            if args.offline:
                raise ValueError('Offline source missing: ' + history.name)
            history.write_bytes(client.get('https://data.sec.gov/submissions/CIK' + cik + '.json'))
        raw_history = history.read_bytes()
        data = json.loads(raw_history)
        assert str(data['cik']).zfill(10) == cik
        recent = data['filings']['recent']
        for period in ['2026-06-30', '2026-09-30']:
            candidates = [i for i, form in enumerate(recent['form'])
                          if form.startswith('13F') and recent['reportDate'][i] == period]
            # No amendment cherry-picking: hold the entire pair if the history
            # includes additional versions requiring the amendment resolver.
            if len(candidates) != 1 or recent['form'][candidates[0]] != '13F-HR':
                raise ValueError(f'Expected one unamended original: {cik} {period}')
            i = candidates[0]
            accession = recent['accessionNumber'][i]
            url = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}.txt'
            path = output / (accession + '.source')
            if not path.exists():
                if accession in saved:
                    raw = (args.staging.parent / (saved[accession] + '.source')).read_bytes()
                    assert hashlib.sha256(raw).hexdigest() == saved[accession]
                else:
                    if args.offline:
                        raise ValueError('Offline source missing: ' + accession)
                    raw = client.get(url)
                path.write_bytes(raw)
            raw = path.read_bytes()
            metadata = {'key': accession, 'cik': cik, 'name': data['name'], 'form': '13F-HR',
                        'filing_date': recent['filingDate'][i], 'url': url}
            _, parsed, reasons = parse_document('sec_13f', raw, metadata)
            assert parsed['metadata']['report_period'] == period and not reasons
            documents.append({'feed': 'sec_13f', 'metadata': metadata, 'source_file': path.name,
                'content_hash': hashlib.sha256(raw).hexdigest(), 'rows': len(parsed['positions']),
                'history_file': history.name, 'history_sha256': hashlib.sha256(raw_history).hexdigest(),
                'report_period': period})
    receipt = {'captured_at': datetime.now(timezone.utc).isoformat(), 'offline': args.offline,
               'documents': documents, 'production_writes': 0}
    name = 'sources-offline.json' if args.offline else 'sources-live.json'
    (output / name).write_text(json.dumps(receipt, indent=2), encoding='utf-8')
    print(json.dumps({'documents': len(documents), 'rows': sum(d['rows'] for d in documents),
                      'offline': args.offline, 'production_writes': 0}))


if __name__ == '__main__':
    main()
