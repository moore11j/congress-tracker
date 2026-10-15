"""Caterpillar consolidated interest from two disjoint original statement rows.

Preparation evidence only. Generic inline parsing still excludes dimensions.
"""
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
import math
import re
from zoneinfo import ZoneInfo
from lxml import html
from app.services import sec_inline_financials as inline
from app.clients.direct_sources import DirectSourceError

VERSION = 'sec_cat_interest_v2'
SYMBOL = 'CAT'
CIK = '0000018230'

class CatInterestError(ValueError):
    pass

PARTS = {'us-gaap:FinancingInterestExpense': ('cat:FinancialProductsMember', 'Interest expense of Financial Products'), 'us-gaap:InterestExpenseNonoperating': ('cat:AllOtherExcludingFinancialProductsMember', 'Interest expense excluding Financial Products')}

def parse(raw, source):
    if not hashlib.sha256(raw).hexdigest() == source['sha256']:
        raise CatInterestError('source_hash')
    standard = inline.parse(raw, cik='0000018230', accession=source['accession'], filed=source['filed'], report_date=source['period'], form=source['form'], url=source['url'])
    tree = html.fromstring(raw)
    if not tree.get('xmlns:cat') == 'http://www.cat.com/' + source['period'].replace('-', ''):
        raise CatInterestError('issuer_namespace')
    if not tree.get('xmlns:xbrldi') == 'http://xbrl.org/2006/xbrldi':
        raise CatInterestError('dimension_namespace')
    if not re.fullmatch(r'http://fasb\.org/srt/[0-9]{4}', tree.get('xmlns:srt', '')):
        raise CatInterestError('axis_namespace')
    contexts = {node.get('id'): node for node in inline.nodes(tree, 'context')}
    units = {node.get('id'): node for node in inline.nodes(tree, 'unit')}
    parts = {}
    for table in tree.xpath('//table'):
        tags = {node.get('name') for node in inline.nodes(table, 'nonfraction')}
        if not set(PARTS) <= tags or 'us-gaap:ProfitLoss' not in tags or 'us-gaap:IncomeTaxExpenseBenefit' not in tags:
            continue
        for node in inline.nodes(table, 'nonfraction'):
            tag = node.get('name')
            if tag not in PARTS:
                continue
            member, label = PARTS[tag]
            tr = next(node.iterancestors('tr'))
            if label not in ' '.join(tr.text_content().split()):
                raise CatInterestError('statement_label')
            context = contexts[node.get('contextref')]
            identifiers = inline.nodes(context, 'identifier')
            if not (len(identifiers) == 1 and identifiers[0].text_content().strip() == '0000018230'):
                raise CatInterestError('issuer_context')
            if not identifiers[0].get('scheme') == 'http://www.sec.gov/CIK':
                raise CatInterestError('issuer_scheme')
            if not (len(inline.nodes(context, 'segment')) == 1 and (not inline.nodes(context, 'scenario'))):
                raise CatInterestError('context_scope')
            members = [n for n in context.iter() if str(n.tag).lower() == 'xbrldi:explicitmember']
            if not (len(members) == 1 and members[0].get('dimension') == 'srt:ProductOrServiceAxis' and (members[0].text_content().strip() == member)):
                raise CatInterestError('dimension_member')
            segment = inline.nodes(context, 'segment')[0]
            if list(segment) != members or len(list(members[0])) != 0:
                raise CatInterestError('additional_dimension_content')
            if any(str(n.tag).lower() == 'xbrldi:typedmember' for n in context.iter()):
                raise CatInterestError('typed_dimension')
            starts, ends = (inline.nodes(context, 'startdate'), inline.nodes(context, 'enddate'))
            if not (len(starts) == len(ends) == 1 and (not inline.nodes(context, 'instant'))):
                raise CatInterestError('duration')
            start, end = (date.fromisoformat(n[0].text_content()) for n in (starts, ends))
            if not (1 <= (end - start).days + 1 <= 380 and str(end) <= source['period']):
                raise CatInterestError('period')
            unit = units[node.get('unitref')]
            if not ([n.text_content() for n in inline.nodes(unit, 'measure')] == ['iso4217:USD'] and (not inline.nodes(unit, 'divide'))):
                raise CatInterestError('unit')
            value = inline._number(node, tree)
            if not (math.isfinite(value) and value >= 0):
                raise CatInterestError('expense_sign')
            fact = dict(value=value, tag=tag, unit='USD', start=str(start), end=str(end), filed=source['filed'], accession=source['accession'], source_cik='0000018230', source_url=source['url'], source_sha256=source['sha256'], context_id=node.get('contextref'), fact_id=node.get('id'), member=member)
            key = (start, end)
            previous = parts.setdefault(key, {}).get(tag)
            if not (previous is None or previous['value'] == value):
                raise CatInterestError('conflicting_component')
            parts[key][tag] = fact
    if not (parts and all((set(pair) == set(PARTS) for pair in parts.values()))):
        raise CatInterestError('incomplete_pair')
    records = {}
    for key, pair in parts.items():
        first = next(iter(pair.values()))
        records[key] = {k: first[k] for k in ('unit', 'start', 'end', 'filed', 'accession', 'source_cik')}
        records[key].update(value=sum((p['value'] for p in pair.values())), tag='reviewed_cat_consolidated_interest', derived=True, inputs=pair, formula='Financial Products interest plus interest explicitly excluding Financial Products', source_sha256=source['sha256'], source_url=source['url'])
    return (records, standard)


def acquire(values, company_raw, *, symbol, cik, observed_at, client):
    if symbol != SYMBOL:
        return None
    proof = {'methodology':VERSION, 'cik':CIK, 'status':'unavailable',
             'documents':[], 'reason':None}
    result = {'evidence':proof, 'records':{}, 'points':{}, 'debt':None}
    try:
        if str(cik).zfill(10) != CIK or observed_at.tzinfo is None:
            raise CatInterestError('issuer_or_observation')
        company = json.loads(company_raw)
        evidence = json.loads(values['source_evidence_json'])
        available = observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
        if (str(company.get('cik','')).zfill(10) != CIK or company.get('tickers',[]).count(SYMBOL) != 1
                or evidence.get('cik') != CIK or evidence.get('available_by') != str(available)):
            raise CatInterestError('issuer_or_availability')
        end = date.fromisoformat(evidence['period_end'])
        if not 0 <= (available-end).days <= 180:
            raise CatInterestError('stale_period')
        ebitda = evidence['leverage_preparation']['ebitda']
        needs_interest = (ebitda.get('status') == 'incomplete'
                          and ebitda.get('missing_components') == ['leverage_interest'])
        needs_debt = (ebitda.get('status') == 'components_complete'
                      and ebitda.get('missing_components') == []
                      and evidence.get('net_debt_policy', {}).get('reason') == 'no_verified_finance_inclusive_debt')
        if not (needs_interest or needs_debt):
            return None
        result['debt_only'] = not needs_interest
        names = ('leverage_net_income','leverage_tax','leverage_da')
        if needs_debt:
            names += ('leverage_interest',)
        components = [ebitda['components'][name] for name in names]
        if any(not point or point['end'] != str(end) or point['start'] != ebitda['period_start'] for point in components):
            raise CatInterestError('incomplete_companions')
        identities = {(p['accession'],p['filed']) for point in components for p in point['inputs']}
        if not 1 <= len(identities) <= 2:
            raise CatInterestError('source_count')
        recent = company.get('filings',{}).get('recent',{})
        arrays = [recent.get(k,[]) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
        if any(not isinstance(a,list) for a in arrays) or len({len(a) for a in arrays}) != 1:
            raise CatInterestError('filing_directory_shape')
        directory = list(zip(*arrays))
        sources = []
        for accession,filed in sorted(identities):
            matches = set(row for row in directory if row[0] == accession and row[2] == filed)
            if len(matches) != 1:
                raise CatInterestError('ambiguous_or_missing_filing')
            acc,form,filing,report,filename = matches.pop()
            if (form not in {'10-K','10-Q','10-K/A','10-Q/A'}
                    or not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',acc)
                    or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',filename)
                    or not date.fromisoformat(report) <= date.fromisoformat(filing) <= available):
                raise CatInterestError('filing_directory_identity')
            if any(r[1] in {form.removesuffix('/A'),form.removesuffix('/A')+'/A'}
                   and r[3] == report and filing < r[2] <= str(available) for r in directory):
                raise CatInterestError('newer_filing_requires_refresh')
            sources.append({'accession':acc,'form':form,'filed':filing,'period':report,
                'url':f'https://www.sec.gov/Archives/edgar/data/{int(CIK)}/{acc.replace("-", "")}/{filename}'})
        # Validate all directory identities before the first optional request.
        for source in sources:
            raw = client.get(source['url'])
            source['sha256'] = hashlib.sha256(raw).hexdigest()
            if needs_interest:
                records, standard = parse(raw, source)
            else:
                # Reported consolidated interest does not require the former
                # dimensional interest layout to remain in future originals.
                records = {}
                standard = inline.parse(raw, cik=CIK, accession=source['accession'],
                    filed=source['filed'], report_date=source['period'],
                    form=source['form'], url=source['url'])
            proof['documents'].append(dict(source))
            if source['period'] == str(end):
                from app.services import sec_cat_debt
                try:
                    result['debt'] = sec_cat_debt.parse(raw, source)
                except (sec_cat_debt.CatDebtError, inline.InlineFactsError,
                        KeyError, TypeError, ValueError, StopIteration) as exc:
                    # Debt is optional independently of the interest proof.
                    result['debt'] = {'methodology':sec_cat_debt.VERSION,
                        'status':'unavailable', 'reason':str(exc) if isinstance(exc,sec_cat_debt.CatDebtError) else type(exc).__name__}
            for key, fact in records.items():
                previous = result['records'].get(key)
                if previous is None or previous['filed'] < fact['filed']:
                    result['records'][key] = fact
                elif previous['filed'] == fact['filed'] and previous != fact:
                    raise CatInterestError('conflicting_originals')
            for tag, concept in standard['facts'].items():
                for point in concept['units'].get('USD',[]):
                    key = (tag,point.get('start'),point['end'],point['filed'],point['accn'],point['val'])
                    provenance = standard['provenance'][(tag,'USD',point.get('start'),point['end'])]
                    if key in result['points'] and result['points'][key] != provenance:
                        raise CatInterestError('conflicting_provenance')
                    result['points'][key] = provenance
        proof.update(status='originals_verified',reason=None)
    except (CatInterestError, inline.InlineFactsError, KeyError, TypeError, ValueError, StopIteration) as exc:
        proof['reason'] = str(exc) if isinstance(exc,CatInterestError) else type(exc).__name__
        result['records'],result['points'],result['debt'] = {},{},None
    except DirectSourceError:
        proof['reason'] = 'source_unavailable'
        result['records'],result['points'],result['debt'] = {},{},None
    return result


def apply(records, supplement, *, symbol, cik, available_by):
    if symbol != SYMBOL or supplement is None:
        return records, None
    proof = deepcopy(supplement['evidence'])
    proof['accepted_periods'] = []
    if (str(cik).zfill(10) != CIK or proof.get('cik') != CIK
            or proof.get('methodology') != VERSION):
        return records, {**proof,'status':'held','reason':'supplement_identity'}
    if proof.get('status') != 'originals_verified':
        return records, proof
    documents = proof.get('documents',[])
    if (not 1 <= len(documents) <= 2 or any(d['filed'] > str(available_by) for d in documents)):
        return records, {**proof,'status':'held','reason':'supplement_availability'}
    allowed = {(d['accession'],d['filed'],d['sha256'],d['url']) for d in documents}
    result = deepcopy(records)
    interest = result.setdefault('leverage_interest',{})
    from app.services.sec_leverage import DURATION
    for key,fact in supplement['records'].items():
        if key in interest:  # Reported observations and conflicts remain authoritative.
            continue
        if (fact.get('source_cik') != CIK or
                (fact['accession'],fact['filed'],fact['source_sha256'],fact['source_url']) not in allowed):
            continue
        companions = []
        for name in DURATION:
            if name == 'leverage_interest':
                continue
            point = result.get(name,{}).get(key)
            if not point:
                break
            identity = tuple(point.get(k) for k in ('tag','start','end','filed','accession','value'))
            source = supplement['points'].get(identity)
            if (not source or source.get('source_cik') != CIK or point.get('source_cik',CIK) != CIK
                    or (point['accession'],point['filed'],source['source_sha256'],source['source_url']) not in allowed
                    or any(point.get(k) != fact.get(k) for k in ('start','end','filed','accession','unit'))
                    or source['source_sha256'] != fact['source_sha256']
                    or any(k in point and point[k] != source[k] for k in ('source_url','source_sha256'))):
                break
            companions.append((point,source))
        if len(companions) != 3:
            continue
        for point,source in companions:
            point.update(source)
        interest[key] = deepcopy(fact)
        proof['accepted_periods'].append({'start':fact['start'],'end':fact['end'],
            'accession':fact['accession'],'value':fact['value']})
    if supplement.get('debt_only'):
        # Bind complete same-period groups together. Partial original coverage
        # cannot degrade an otherwise valid reported EBITDA decomposition.
        verified_periods = []
        keys = set.intersection(*(set(result.get(name, {})) for name in DURATION))
        for key in sorted(keys):
            bound = []
            for name in DURATION:
                point = result[name].get(key)
                if not point:
                    break
                identity = tuple(point.get(k) for k in ('tag','start','end','filed','accession','value'))
                origin = supplement['points'].get(identity)
                if (not origin or origin.get('source_cik') != CIK
                        or point.get('source_cik', CIK) != CIK or point.get('unit') != 'USD'
                        or (point['accession'],point['filed'],origin['source_sha256'],origin['source_url']) not in allowed
                        or any(k in point and point[k] != origin[k] for k in ('source_url','source_sha256'))):
                    break
                bound.append((point,origin))
            if (len(bound) != len(DURATION) or len({
                    (point['filed'],point['accession'],origin['source_sha256'])
                    for point,origin in bound}) != 1):
                continue
            for point,origin in bound:
                point.update(origin)
            verified_periods.append({'start':str(key[0]),'end':str(key[1])})
        proof['reported_components_verified'] = verified_periods
    proof['status'] = 'prepared' if proof['accepted_periods'] else 'held'
    from app.services import sec_cat_debt
    result, debt_evidence = sec_cat_debt.apply(result, supplement.get('debt'),
        available_by=available_by, allowed=allowed)
    if debt_evidence is not None:
        proof['debt_reconciliation'] = debt_evidence
        if debt_evidence['status'] == 'prepared':
            proof['status'] = 'prepared'
    return result, proof


def supplement(values, facts_raw, company_raw, *, symbol, cik, observed_at, client,
               history_raw=None, filing_supplement=None):
    if symbol != SYMBOL:
        return values
    acquired = acquire(values,company_raw,symbol=symbol,cik=cik,observed_at=observed_at,client=client)
    if acquired is None:
        return values
    from app.services.sec_fundamentals import project_fundamentals
    return project_fundamentals(facts_raw,company_raw,symbol=symbol,cik=cik,
        observed_at=observed_at,history_raw=history_raw,filing_supplement=filing_supplement,
        interest_supplement=acquired)
