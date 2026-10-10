"""Reviewed one-for-one security continuity without rewriting reported holdings."""
from datetime import date,datetime,timezone
import hashlib,html,json,re
from pathlib import Path
from functools import lru_cache

def reviewed_transitions(manifest_path):
    path=Path(manifest_path).resolve(strict=True)
    data=json.loads(path.read_bytes())
    if data['version']!=1:raise ValueError('Unsupported transition manifest')
    observed=datetime.fromisoformat(data['reviewed_at'])
    if observed.tzinfo is None:raise ValueError('Missing actual review timestamp')
    rows=[];seen=set()
    def norm(value):return ' '.join(value.casefold().split())
    for item in data['transitions']:
        if item['share_ratio']!='1':raise ValueError('Only one-for-one continuity is supported')
        if not all(re.fullmatch(r'[A-Z0-9]{9}',item[k]) for k in ('old_cusip','new_cusip')):raise ValueError('Invalid security')
        if item['old_cusip']==item['new_cusip'] or item['symbol'] in seen:raise ValueError('Ambiguous transition')
        date.fromisoformat(item['effective_date'])
        texts=[]
        for source in item['sources']:
            target=(path.parent/source['source_file']).resolve(strict=True)
            if target.parent!=path.parent or target.stat().st_size>100000:raise ValueError('Unbounded transition source')
            raw=target.read_bytes()
            if hashlib.sha256(raw).hexdigest()!=source['sha256']:raise ValueError('Transition source checksum changed')
            texts.append(html.unescape(re.sub(r'<[^>]+>',' ',raw.decode('utf-8'))))
        combined=norm(' '.join(texts))
        for value in [item['old_cusip'],item['new_cusip'],*item['required_fragments']]:
            if norm(value) not in combined:raise ValueError('Transition fact missing from captured source')
        rows.append({**item,'source_availability_basis':'evidence_reviewed_at','observed_at':observed.isoformat()})
        seen.add(item['symbol'])
    return rows

def comparison_plan(before,after,*,prior_period,current_period,observed_by,transitions):
    """Positions are symbol/CUSIP dictionaries; options remain outside this plan."""
    prior_period=date.fromisoformat(prior_period);current_period=date.fromisoformat(current_period)
    observed=datetime.fromisoformat(observed_by)
    if observed.tzinfo is None:raise ValueError('Comparison observation needs timezone')
    if prior_period>=current_period:raise ValueError('Invalid reporting periods')
    def groups(rows):
        result={}
        for row in rows:
            if row.get('put_call'):continue
            if not row.get('symbol') or not row.get('cusip'):raise ValueError('Unmapped position')
            result.setdefault(row['symbol'],set()).add(row['cusip'])
        return result
    old,new=groups(before),groups(after);aliases={};proof=[]
    for symbol in sorted(old.keys() & new.keys()):
        if old[symbol]==new[symbol]:continue
        matches=[r for r in transitions if r['symbol']==symbol and old[symbol]=={r['old_cusip']} and new[symbol]=={r['new_cusip']}
                 and prior_period<date.fromisoformat(r['effective_date'])<=current_period
                 and datetime.fromisoformat(r['observed_at'])<=observed]
        if len(matches)!=1:return {'status':'held','reason':'unverified_security_transition','symbol':symbol}
        item=matches[0];aliases[item['old_cusip']]=item['new_cusip'];proof.append(item)
    return {'status':'ready','prior_cusip_aliases':aliases,'evidence':proof}


def _period(year, quarter):
    import calendar
    month = int(quarter) * 3
    return date(int(year), month, calendar.monthrange(int(year), month)[1])


def _rows(positions):
    return [{'symbol': p.normalized_symbol, 'cusip': p.cusip, 'put_call': p.put_call} for p in positions]


@lru_cache(maxsize=1)
def _registry():
    return reviewed_transitions(Path(__file__).resolve().parents[2] / 'config/security_transitions/manifest.json')


def filing_comparison_plan(filing, prior_positions, current_positions, *, observed_by=None):
    current_period = _period(filing.report_year, filing.report_quarter)
    prior_year, prior_quarter = ((filing.report_year - 1, 4) if filing.report_quarter == 1
                               else (filing.report_year, filing.report_quarter - 1))
    return comparison_plan(_rows(prior_positions), _rows(current_positions),
        prior_period=_period(prior_year, prior_quarter).isoformat(),
        current_period=current_period.isoformat(),
        observed_by=observed_by or datetime.now(timezone.utc).isoformat(), transitions=_registry())


def prior_comparison_key(filing, position, key):
    """Use only the reviewed continuity recorded on this exact direct filing."""
    if position.put_call or key[0] != 'cusip':
        return key
    proof = json.loads(filing.raw_metadata_json or '{}').get('_walnut_direct_13f', {})
    evidence = proof.get('security_transition_evidence', [])
    if not evidence:
        return key
    registry = _registry()
    current_period = _period(filing.report_year, filing.report_quarter)
    prior_year, prior_quarter = ((filing.report_year - 1, 4) if filing.report_quarter == 1
                               else (filing.report_year, filing.report_quarter - 1))
    prior_period = _period(prior_year, prior_quarter)
    now = datetime.now(timezone.utc)
    seen = set()
    for item in evidence:
        if (item not in registry or item['old_cusip'] in seen
                or not prior_period < date.fromisoformat(item['effective_date']) <= current_period
                or datetime.fromisoformat(item['observed_at']) > now):
            raise ValueError('Unverified recorded security continuity')
        seen.add(item['old_cusip'])
    for item in evidence:
        if item['old_cusip'] == position.cusip:
            if (position.normalized_symbol != item['symbol']
                    or (position.report_year, position.report_quarter) != (prior_year, prior_quarter)):
                raise ValueError('Security continuity position identity changed')
            return (key[0], item['new_cusip'], *key[2:])
    return key
