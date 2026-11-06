"""Bounded complete-source historical position plans and in-memory rehearsals.

No production apply entry point, source switch, schedule or customer delivery.
Exact original evidence and complete filing populations guard every correction.
"""
from datetime import date, datetime
from sqlalchemy import select
from app.models import InstitutionalFiling, InstitutionalPosition
from app.services.direct_feed_worker import _source_url
from app.services.direct_13f_evidence import value_consistency_issues
from app.services.direct_feed_rehearsal import snapshot
from app.services.direct_feed_store import reconcile_institutional

MAX_SOURCE_BYTES = 20_000_000
MAX_POSITIONS = 10_000

from collections import defaultdict, Counter
from decimal import Decimal
import base64, copy, hashlib, json, math, xml.etree.ElementTree as ET
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_rehearsal import digest

def text(node, name):
    return (node.findtext('.//{*}' + name) or '').strip()

def value_kind(stored, exact):
    if stored is None or not math.isfinite(stored) or (not exact.is_finite()) or (exact < 0):
        return None
    current = Decimal(str(stored))
    if current == exact:
        return 'unchanged'
    if current * 1000 == exact:
        return 'verified_unit_difference'
    if exact > 0 and exact == exact.to_integral_value() and (exact <= 2 ** 53) and (abs(current - exact) < Decimal('0.005')) and (abs(stored - float(exact)) <= math.ulp(float(exact))) and (float(exact / Decimal(1000)) * 1000 == stored):
        return 'exact_scaled_float_roundtrip'
    return None

def voting_parts(value):
    if isinstance(value, str):
        value = json.loads(value)
    if value is None:
        return []
    if set(value) == {'components'}:
        return [item for part in value['components'] for item in voting_parts(part)]
    if not set(value) == {'sole', 'shared', 'none'}:
        raise ValueError('Complete original or canonical filing evidence differs')
    return [value]

def totals(parts):
    return {k: sum((Decimal(str(p[k])) for p in parts), Decimal(0)) for k in ('sole', 'shared', 'none')}

def _source_proposal(item):
    raw = base64.b64decode(item['raw'])
    if not hashlib.sha256(raw).hexdigest() == item['content_hash']:
        raise ValueError('Complete original or canonical filing evidence differs')
    _, parsed, reasons = parse_document('sec_13f', raw, item['metadata'])
    if not (not reasons and parsed == item['parsed']):
        raise ValueError('Complete original or canonical filing evidence differs')
    filing = item['filing']
    meta = parsed['metadata']
    if not (not filing['is_amendment'] and filing['superseded_by'] is None):
        raise ValueError('Complete original or canonical filing evidence differs')
    if not (filing['accession_number'] == meta['key'] and filing['cik'].lstrip('0') == meta['cik'].lstrip('0')):
        raise ValueError('Complete original or canonical filing evidence differs')
    if not (str(filing['filing_date']) == meta['filing_date'] and str(filing['report_period_end']) == meta['report_period']):
        raise ValueError('Complete original or canonical filing evidence differs')
    roots = [ET.fromstring(block.split('</XML>', 1)[0].strip()) for block in raw.decode('utf-8').split('<XML>')[1:]]
    covers = [r for r in roots if r.tag.rsplit('}', 1)[-1] == 'edgarSubmission']
    tables = [r for r in roots if r.tag.rsplit('}', 1)[-1] == 'informationTable']
    if not len(covers) == len(tables) == 1:
        raise ValueError('Complete original or canonical filing evidence differs')
    nodes = tables[0].findall('.//{*}infoTable')
    if not int(text(covers[0], 'tableEntryTotal')) == len(nodes) == len(parsed['positions']):
        raise ValueError('Complete original or canonical filing evidence differs')
    if not sum((Decimal(text(n, 'value')) for n in nodes), Decimal(0)) == Decimal(text(covers[0], 'tableValueTotal')):
        raise ValueError('Complete original or canonical filing evidence differs')
    groups = defaultdict(list)
    existing = defaultdict(list)
    for row in parsed['positions']:
        groups[row['cusip'].upper(), (row.get('putCall') or '').upper()].append(row)
    for row in item['positions']:
        existing[(row.get('cusip') or '').upper(), (row.get('put_call') or '').upper()].append(row)
    if not groups.keys() == existing.keys():
        raise ValueError('Complete original or canonical filing evidence differs')
    operations = []
    kinds = Counter()
    for key, rows in groups.items():
        if not len(existing[key]) == 1:
            raise ValueError('Complete original or canonical filing evidence differs')
        before = existing[key][0]
        after = copy.deepcopy(before)
        shares = sum((Decimal(str(r['shares'])) for r in rows), Decimal(0))
        value = sum((Decimal(str(r['valueUsd'])) for r in rows), Decimal(0))
        kind = value_kind(before['value_usd'], value)
        if Decimal(str(before['shares'])) != shares:
            if not (key[1] in {'PUT', 'CALL'} and before['portfolio_weight'] is None and (before['ownership_pct'] is None)):
                raise ValueError('Complete original or canonical filing evidence differs')
            unique = {}
            for row in rows:
                index = int(row['source_line_ref']) - 1
                node = nodes[index]
                if not (text(node, 'cusip').upper(), text(node, 'putCall').upper()) == key:
                    raise ValueError('Complete original or canonical filing evidence differs')
                unique[ET.canonicalize(ET.tostring(node, encoding='unicode'), strip_text=True)] = row
            if not len(unique) < len(rows):
                raise ValueError('Complete original or canonical filing evidence differs')
            if not sum((Decimal(str(r['shares'])) for r in unique.values()), Decimal(0)) == Decimal(str(before['shares'])):
                raise ValueError('Complete original or canonical filing evidence differs')
            if not sum((Decimal(str(r['valueUsd'])) for r in unique.values()), Decimal(0)) == Decimal(str(before['value_usd'])):
                raise ValueError('Complete original or canonical filing evidence differs')
            if not totals(voting_parts(before['voting_authority'])) == totals([r['votingAuthority'] for r in unique.values()]):
                raise ValueError('Complete original or canonical filing evidence differs')
            after['shares'] = float(shares)
            after['voting_authority'] = json.dumps({'components': [r['votingAuthority'] for r in rows]}, sort_keys=True)
            if not totals(voting_parts(after['voting_authority'])) == totals([r['votingAuthority'] for r in rows]):
                raise ValueError('Complete original or canonical filing evidence differs')
            kind = 'complete_original_repeated_xml_rows'
        if not kind is not None:
            raise ValueError('Entire filing held: numeric difference lacks exact accepted explanation')
        after['value_usd'] = float(value)
        changes = {k: v for k, v in after.items() if before[k] != v}
        if changes:
            if not set(changes) <= {'shares', 'value_usd', 'voting_authority'}:
                raise ValueError('Complete original or canonical filing evidence differs')
            operations.append(dict(id=before['id'], before=before, after=after, changes=changes, kind=kind, source_sha256=item['content_hash'], accession=meta['key'], source_rows=[r['source_line_ref'] for r in rows]))
            kinds[kind] += 1
    return dict(filing_id=filing['id'], accession=meta['key'], source_sha256=item['content_hash'], source_rows=len(nodes), canonical_positions=len(item['positions']), guard_sha256=digest(dict(filing=filing, positions=item['positions'])), operations=operations, kinds=dict(kinds))


def plan_historical_positions(document, filing, positions, *, publish_since,
                              comparison_documents, prepared_evidence=None):
    if not isinstance(publish_since, date) or isinstance(publish_since, datetime):
        raise ValueError('An explicit historical publication boundary is required')
    if document.get('feed') != 'sec_13f' or not isinstance(document.get('raw'), bytes):
        raise ValueError('Expected complete SEC 13F bytes')
    if not 0 < len(document['raw']) <= MAX_SOURCE_BYTES or not 0 < len(positions) <= MAX_POSITIONS:
        raise ValueError('Historical source exceeds the reviewed bounds or has no positions')
    _source_url(document['metadata'])
    _, parsed, reasons = parse_document('sec_13f', document['raw'], document['metadata'])
    if reasons:
        raise ValueError('Original source is held: ' + '; '.join(reasons))
    metadata = parsed['metadata']
    if (filing['form_type'] != '13F-HR' or filing['filing_date'] >= publish_since.isoformat()
            or filing['report_year'] != metadata['report_year']
            or filing['report_quarter'] != metadata['report_quarter']):
        raise ValueError('Filing is not the matching historical original')
    if json.loads(filing.get('raw_metadata_json') or '{}').get('_walnut_direct_13f'):
        raise ValueError('Source-owned filing requires its publication receipt reconciliation')
    if not comparison_documents:
        raise ValueError('Independent whole-source comparison evidence is required')
    if value_consistency_issues(parsed, comparison_documents, prepared=prepared_evidence):
        raise ValueError('Independent SEC evidence holds the original value units')
    classes = defaultdict(set)
    for row in parsed['positions']:
        option = (row.get('putCall') or '').upper()
        if row.get('shareType') != 'SH' or option not in {'', 'PUT', 'CALL'}:
            raise ValueError('Principal or unknown option semantics require separate evidence')
        classes[(row['cusip'].upper(), option)].add((row.get('titleOfClass') or '').strip().upper())
    if any(len(labels) != 1 for labels in classes.values()):
        raise ValueError('Conflicting security classes cannot be consolidated')
    if len({row['id'] for row in positions}) != len(positions):
        raise ValueError('Duplicate canonical position identity')
    for row in positions:
        if (row['filing_id'] != filing['id'] or row['cik'].lstrip('0') != filing['cik'].lstrip('0')
                or row['report_year'] != filing['report_year'] or row['report_quarter'] != filing['report_quarter']):
            raise ValueError('Canonical position does not belong to the original filing')
    item = dict(raw=base64.b64encode(document['raw']).decode(), metadata=document['metadata'],
                content_hash=document['content_hash'], parsed=parsed, filing=filing, positions=positions)
    proposal = _source_proposal(item)
    for op in proposal['operations']:
        if op['before']['portfolio_weight'] is not None or op['before']['ownership_pct'] is not None:
            raise ValueError('Existing weight or ownership dependencies require a separate reviewed plan')
    plan = dict(version=1, filing=copy.deepcopy(filing), positions=copy.deepcopy(positions),
                source_metadata_sha256=digest(document['metadata']), publish_since=publish_since.isoformat(),
                proposal=proposal, scope='complete historical positions only; downstream rebaseline required')
    return {**plan, 'sha256': digest(plan)}


def apply_position_rehearsal(db, document, plan, *, expected_plan_hash,
                            comparison_documents, prepared_evidence=None):
    bind = db.get_bind()
    if bind.dialect.name != 'sqlite' or bind.url.database != ':memory:':
        raise ValueError('Source correction rehearsal requires in-memory SQLite')
    if db.new or db.dirty or db.deleted:
        raise ValueError('Source correction session has unrelated pending changes')
    if (not expected_plan_hash or plan['sha256'] != expected_plan_hash
            or expected_plan_hash != digest({k:v for k,v in plan.items() if k != 'sha256'})):
        raise ValueError('Reviewed source plan changed')
    rebuilt = plan_historical_positions(document, plan['filing'], plan['positions'],
        publish_since=date.fromisoformat(plan['publish_since']), comparison_documents=comparison_documents,
        prepared_evidence=prepared_evidence)
    if rebuilt != plan:
        raise ValueError('Original evidence no longer reproduces the reviewed plan')
    filing = db.get(InstitutionalFiling, plan['filing']['id'], populate_existing=True)
    if filing is None or snapshot(filing) != plan['filing']:
        raise ValueError('Historical filing changed since planning')
    rows = list(db.scalars(select(InstitutionalPosition).where(
        InstitutionalPosition.filing_id == filing.id).order_by(InstitutionalPosition.id)))
    expected = sorted(plan['positions'], key=lambda row: row['id'])
    if [r.id for r in rows] != [r['id'] for r in expected]:
        raise ValueError('Complete canonical position population changed')
    operations = {op['id']:op for op in plan['proposal']['operations']}
    pending = []
    for row, before in zip(rows, expected, strict=True):
        current = snapshot(row)
        op = operations.get(row.id)
        after = op['after'] if op else before
        if current not in (before, after):
            raise ValueError('Historical position changed since review')
        if op and current != after:
            pending.append((row,op))
    with db.begin_nested():
        for row, op in pending:
            if not set(op['changes']) <= {'value_usd', 'shares', 'voting_authority'}:
                raise ValueError('Unsupported historical correction field')
            for field, value in op['changes'].items():
                setattr(row, field, value)
        db.flush()
        _, parsed, reasons = parse_document('sec_13f', document['raw'], document['metadata'])
        reconciled = reconcile_institutional(db, parsed)
        if reasons or any(reconciled.get(k) for k in ('unmatched','ambiguous','existing_only_ids')):
            raise ValueError('Correction failed complete original reconciliation')
    return dict(updated=len(pending), skipped=len(operations)-len(pending), inserted=0, deleted=0,
                source_sha256=document['content_hash'], plan_sha256=expected_plan_hash, production_writes=0)
