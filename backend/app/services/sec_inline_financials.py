"""Bounded standard-fact recovery when companyfacts lags a filed US GAAP report."""
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
import re
from zoneinfo import ZoneInfo
from lxml import html
from app.services.sec_fundamentals import SecFundamentalsError

VERSION = 'sec_inline_standard_facts_v1'
FORMS = {'10-Q', '10-K', '10-Q/A', '10-K/A'}
MAX_INLINE_BYTES = 20_000_000
LEGACY_TRR = 'http://www.xbrl.org/inlineXBRL/transformation/2015-02-26'
TRR = {'http://www.xbrl.org/inlineXBRL/transformation/2020-02-12',
       'http://www.xbrl.org/inlineXBRL/transformation/2022-02-16'}


class InlineFactsError(SecFundamentalsError):
    pass


def nodes(root, local):
    return [n for n in root.iter() if isinstance(n.tag, str) and n.tag.lower() == ('ix:' if local == 'nonfraction' else 'xbrli:')+local]


def _number(node, tree):
    value = node.text_content().strip()
    transform = node.get('format')
    if transform:
        prefix, _, local = transform.partition(':')
        namespace = tree.get('xmlns:'+prefix)
        if namespace == LEGACY_TRR and local == 'numdotdecimal':
            # TRR 3 dot-decimal uses commas or spaces as grouping separators.
            if not re.fullmatch(r'(?:[0-9]+|[0-9]{1,3}(?:[, \u00a0][0-9]{3})+)(?:\.[0-9]+)?', value):
                raise InlineFactsError('unsupported_numeric_format')
            value = value.replace(' ', '').replace('\u00a0', '')
            local = 'num-dot-decimal'
        elif namespace not in TRR:
            raise InlineFactsError('unsupported_numeric_namespace')
    else:
        local = None
    if node.get('continuedat') or node.get('xsi:nil') or node.get('sign') not in {None, '-'}:
        raise InlineFactsError('unsupported_numeric_fact')
    if local == 'fixed-zero':
        value = '0'  # Explicit reported transformation, not inferred missing data.
    elif local == 'num-dot-decimal':
        if not re.fullmatch(r'(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\.[0-9]+)?', value):
            raise InlineFactsError('unsupported_numeric_format')
        value = value.replace(',', '')
    elif local is not None or not re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', value):
        raise InlineFactsError('unsupported_numeric_format')
    scale = node.get('scale', '0')
    if not re.fullmatch(r'-?[0-9]', scale):
        raise InlineFactsError('unsupported_numeric_scale')
    try:
        result = float(Decimal(value) * Decimal(10)**int(scale))
    except (InvalidOperation, ValueError, OverflowError) as exc:
        raise InlineFactsError('invalid_numeric_fact') from exc
    if not math.isfinite(result) or abs(result) > 1e16:
        raise InlineFactsError('numeric_fact_out_of_bounds')
    return -result if node.get('sign') == '-' else result



def _consistent_precision(previous, previous_decimals, value, decimals):
    """Select a more precise auxiliary fact only when its interval is contained.

    Rounded narrative duplicates are not restatements. Unknown precision or
    genuinely conflicting reported values remain held by the caller.
    """
    def precision(raw):
        if raw == 'INF':
            return 100
        if not isinstance(raw, str) or not re.fullmatch(r'-?[0-9]{1,2}', raw):
            return None
        number = int(raw)
        return number if -20 <= number <= 20 else None
    old, new = precision(previous_decimals), precision(decimals)
    if old is None or new is None or old == new:
        return None
    fine, fine_precision, coarse, coarse_precision = (
        (value, new, previous, old) if new > old else (previous, old, value, new))
    with localcontext() as context:
        context.prec = 64
        fine, coarse = Decimal(str(fine)), Decimal(str(coarse))
        fine_error = Decimal(0) if fine_precision == 100 else Decimal(10)**(-fine_precision)/2
        coarse_error = Decimal(0) if coarse_precision == 100 else Decimal(10)**(-coarse_precision)/2
        if fine-fine_error >= coarse-coarse_error and fine+fine_error <= coarse+coarse_error:
            return new > old
    return None


def parse(raw, *, cik, accession, filed, report_date, form, url, basis=None):
    from app.services.sec_fundamentals import TAGS
    namespace = basis['namespace'] if basis else 'us-gaap'
    currency = basis['currency'] if basis else 'USD'
    tags = basis['tags'] if basis else TAGS
    if namespace not in {'us-gaap', 'ifrs-full'} or not re.fullmatch(r'[A-Z]{3}', currency):
        raise InlineFactsError('unsupported_accounting_basis')
    if not raw or len(raw) > MAX_INLINE_BYTES:
        raise InlineFactsError('filing_size')
    source_sha256 = hashlib.sha256(raw).hexdigest()
    tree = html.fromstring(raw)
    namespace_pattern = r'https?://fasb\.org/us-gaap/[0-9]{4}' if namespace == 'us-gaap' else r'https?://xbrl\.ifrs\.org/taxonomy/[0-9]{4}-[0-9]{2}-[0-9]{2}/ifrs-full'
    if (not re.fullmatch(namespace_pattern, tree.get('xmlns:'+namespace, ''))
            or not re.fullmatch(r'https?://xbrl.sec.gov/dei/[0-9]{4}', tree.get('xmlns:dei', ''))
            or tree.get('xmlns:ix') != 'http://www.xbrl.org/2013/inlineXBRL'
            or tree.get('xmlns:xbrli') != 'http://www.xbrl.org/2003/instance'
            or tree.get('xmlns:iso4217') != 'http://www.xbrl.org/2003/iso4217'):
        raise InlineFactsError('filing_namespaces')
    # Additional aliases (for example a local xlink prefix) do not alter the
    # fixed root prefixes used by this reader. Rebinding any root prefix does.
    # Transforms still require an explicitly trusted root namespace.
    for n in tree.iter():
        if any(value != tree.get(name) for name, value in n.attrib.items()
               if name.startswith('xmlns:') and name in tree.attrib):
            raise InlineFactsError('namespace_redefinition')
    cik = str(cik).zfill(10)
    contexts = {}
    for n in nodes(tree, 'context'):
        key = n.get('id')
        if not key or key in contexts:
            raise InlineFactsError('duplicate_context')
        contexts[key] = None
        if nodes(n, 'segment') or nodes(n, 'scenario'):
            continue
        ids = nodes(n, 'identifier')
        if len(ids) != 1 or ids[0].get('scheme') != 'http://www.sec.gov/CIK' or ids[0].text_content().strip().zfill(10) != cik:
            continue
        starts, ends, instants = nodes(n, 'startdate'), nodes(n, 'enddate'), nodes(n, 'instant')
        if len(instants) == 1 and not starts and not ends:
            period = (None, date.fromisoformat(instants[0].text_content()))
        elif len(starts) == len(ends) == 1 and not instants:
            period = (date.fromisoformat(starts[0].text_content()), date.fromisoformat(ends[0].text_content()))
            if not 1 <= (period[1]-period[0]).days+1 <= 380:
                continue
        else:
            continue
        if str(period[1]) <= report_date:
            contexts[key] = period
    identity, forms, dates = set(), set(), set()
    for n in tree.iter():
        if n.tag != 'ix:nonnumeric' or contexts.get(n.get('contextref')) is None:
            continue
        name = n.get('name')
        if name == 'dei:EntityCentralIndexKey':
            identity.add(n.text_content().strip().zfill(10))
        if name == 'dei:DocumentType':
            forms.add(n.text_content().strip())
        if name == 'dei:DocumentPeriodEndDate':
            value = ' '.join(n.text_content().split())
            transform = n.get('format')
            if transform:
                prefix, _, local = transform.partition(':')
                date_namespace = tree.get('xmlns:'+prefix)
                if date_namespace == LEGACY_TRR and local == 'datemonthdayyearen':
                    local = 'date-monthname-day-year-en'
                elif date_namespace not in TRR:
                    raise InlineFactsError('unsupported_report_date')
                if local == 'date-monthname-day-year-en':
                    value = str(datetime.strptime(value, '%B %d, %Y').date())
                elif local == 'date-month-day-year':
                    # XBRL TRR 4/5: numeric M/D/Y, nonnumeric separators,
                    # and one/two-digit years in the 2000s (not strptime's pivot).
                    match = re.fullmatch(r'([0-9]{1,2})[^0-9]+([0-9]{1,2})[^0-9]+([0-9]{1,2}|[0-9]{4})', value)
                    if match is None:
                        raise InlineFactsError('unsupported_report_date')
                    month, day, year = match.groups()
                    try:
                        value = str(date(int(year) + (2000 if len(year) < 4 else 0), int(month), int(day)))
                    except ValueError as exc:
                        raise InlineFactsError('unsupported_report_date') from exc
                else:
                    raise InlineFactsError('unsupported_report_date')
            dates.add(str(date.fromisoformat(value)))
    if identity != {cik} or forms != {form} or dates != {report_date}:
        raise InlineFactsError('filing_identity')
    units = {}; unit_ids = set()
    for n in nodes(tree, 'unit'):
        key = n.get('id')
        if not key or key in unit_ids:
            raise InlineFactsError('duplicate_unit')
        unit_ids.add(key)
        measures = [x.text_content().strip() for x in nodes(n, 'measure')]
        if measures == ['iso4217:'+currency] and not nodes(n, 'divide'):
            units[key] = currency
        elif measures == ['iso4217:'+currency, 'xbrli:shares'] and len(nodes(n, 'divide')) == 1 and len(nodes(n, 'unitnumerator')) == len(nodes(n, 'unitdenominator')) == 1:
            numerator = nodes(n, 'unitnumerator')[0]
            denominator = nodes(n, 'unitdenominator')[0]
            if ([m.text_content().strip() for m in nodes(numerator, 'measure')] == ['iso4217:'+currency]
                    and [m.text_content().strip() for m in nodes(denominator, 'measure')] == ['xbrli:shares']):
                units[key] = currency+'/shares'
    wanted = {tag for concepts in tags.values() for tag in concepts} | ({'EarningsPerShareDiluted'} if basis is None else set())
    auxiliary = set()
    if basis is None:
        from app.services.sec_leverage import TAGS as leverage_tags
        from app.services.sec_parent_income import TAGS as parent_income_tags
        auxiliary = {tag for concepts in {**leverage_tags, **parent_income_tags}.values() for tag in concepts} - wanted
        wanted |= auxiliary
    records = {}; provenance = {}; precisions = {}; rounded_duplicates = []; auxiliary_holds = {}
    for n in nodes(tree, 'nonfraction'):
        name = n.get('name', '')
        if not name.startswith(namespace+':') or name.split(':', 1)[1] not in wanted:
            continue
        tag = name.split(':', 1)[1]
        period, unit = contexts.get(n.get('contextref')), units.get(n.get('unitref'))
        if period is None or unit != (currency+'/shares' if tag == 'EarningsPerShareDiluted' else currency):
            continue
        start, end = period
        key = (tag, unit, str(start) if start else None, str(end))
        if key in auxiliary_holds:
            continue
        try:
            value = _number(n, tree)
        except InlineFactsError as exc:
            if tag not in auxiliary:
                raise
            auxiliary_holds[key] = str(exc)
            records.pop(key, None); provenance.pop(key, None); precisions.pop(key, None)
            continue
        fact = {'end': str(end), 'val': value, 'filed': filed, 'form': form, 'accn': accession}
        if start:
            fact['start'] = str(start)
        if key in records and records[key]['val'] != value:
            preference = _consistent_precision(records[key]['val'], precisions[key], value, n.get('decimals')) if tag in auxiliary else None
            if preference is None:
                if tag not in auxiliary:
                    raise InlineFactsError('conflicting_reported_facts')
                auxiliary_holds[key] = 'conflicting_reported_facts'
                records.pop(key, None); provenance.pop(key, None); precisions.pop(key, None)
                continue
            rounded_duplicates.append({'tag':tag, 'unit':unit, 'start':key[2], 'end':key[3],
                'values':[records[key]['val'], value], 'decimals':[precisions[key], n.get('decimals')],
                'selected_value':value if preference else records[key]['val']})
            if not preference:
                continue
        elif key in records and tag in auxiliary and records[key]['val'] == value:
            # Keep the existing finer provenance when equal-value duplicates
            # have known unequal precision; missing decimals grant no preference.
            preference = _consistent_precision(value, precisions[key], value, n.get('decimals'))
            if preference is False:
                continue
        records[key] = fact
        precisions[key] = n.get('decimals')
        provenance[key] = {'source_kind':'sec_inline_xbrl', 'source_url':url,
            'source_sha256':source_sha256, 'source_cik':cik,
            'context_id':n.get('contextref'), 'fact_id':n.get('id')}
    if not any(k[0] in tags['revenue'] and k[2] is not None and k[3] == report_date for k in records):
        raise InlineFactsError('latest_reported_revenue_unavailable')
    gaap = {}
    for (tag, unit, start, end), fact in records.items():
        gaap.setdefault(tag, {'units':{}})['units'].setdefault(unit, []).append(fact)
    return {'facts':gaap, 'provenance':provenance, 'evidence':{'methodology':VERSION,
        'cik':cik, 'accession':accession, 'filed':filed, 'report_date':report_date, 'form':form,
        'url':url, 'sha256':source_sha256, 'reported_facts':len(records),
        'namespace':namespace, 'currency':currency,
        **({'auxiliary_rounded_duplicates':rounded_duplicates} if rounded_duplicates else {}),
        **({'auxiliary_holds':[{'tag':k[0], 'unit':k[1], 'start':k[2], 'end':k[3], 'reason':v}
            for k,v in sorted(auxiliary_holds.items(), key=lambda item: repr(item[0]))]} if auxiliary_holds else {})}}


def acquire(facts_raw, company_raw, *, symbol, cik, observed_at, client):
    from app.services import sec_fundamentals as sec
    from app.utils.symbols import canonical_symbol
    if observed_at.tzinfo is None:
        raise InlineFactsError('naive_observation')
    payload, company = json.loads(facts_raw), json.loads(company_raw)
    cik = str(cik).zfill(10)
    if (any(str(x.get('cik','')).zfill(10) != cik for x in (payload, company))
            or [canonical_symbol(s) for s in company.get('tickers',[])].count(canonical_symbol(symbol)) != 1):
        raise InlineFactsError('company_identity')
    available = observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
    from app.services.sec_reporting_policy import forms_for, periodic_document
    forms = forms_for(symbol, cik)
    ends = [end for name in sec.TAGS for (_,end),value in sec._records(payload,name,available,forms=forms).items() if value]
    if not ends:
        return None  # Unsupported taxonomy/currency requires a separate adapter.
    latest_fact = max(ends)
    recent = company.get('filings',{}).get('recent',{})
    keys = ['accessionNumber','form','filingDate','reportDate','primaryDocument']
    arrays = [recent.get(k,[]) for k in keys]
    if len({len(a) for a in arrays}) != 1:
        raise InlineFactsError('filing_directory_shape')
    candidates = []
    for accession, form, filed, report, filename in zip(*arrays):
        end, filing = sec._day(report), sec._day(filed)
        if (end and filing and latest_fact < end <= filing <= available
                and periodic_document(symbol, cik, form, report, filename)):
            candidates.append((str(end),str(filing),accession,form,filename))
    if not candidates:
        return None
    latest = max((r[0],r[1]) for r in candidates)
    candidates = {r for r in candidates if r[:2] == latest}
    if len(candidates) != 1:
        raise InlineFactsError('ambiguous_latest_filing')
    report, filed, accession, form, filename = next(iter(candidates))
    if (not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',accession)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',filename)):
        raise InlineFactsError('filing_directory_identity')
    url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{filename}'
    return parse(client.get(url),cik=cik,accession=accession,filed=filed,report_date=report,form=form,url=url)


def merge(payload, supplement, *, available_by):
    if supplement is None:
        return payload
    evidence = supplement['evidence']
    if str(payload.get('cik','')).zfill(10) != evidence['cik'] or evidence['filed'] > str(available_by):
        raise InlineFactsError('supplement_identity_or_availability')
    merged = deepcopy(payload)
    gaap = merged.setdefault('facts',{}).setdefault(evidence.get('namespace', 'us-gaap'),{})
    for tag, content in supplement['facts'].items():
        for unit, facts in content['units'].items():
            gaap.setdefault(tag,{'units':{}})['units'].setdefault(unit,[]).extend(facts)
    return merged


def annotate(records, supplement, *, metrics=None):
    if supplement is None:
        return records
    from app.services import sec_fundamentals as sec
    metrics = metrics or {name:(tags,'USD') for name,tags in sec.TAGS.items()}
    for name, series in records.items():
        # A held optional fact must not fall back to an older companyfacts value
        # for the same period or be resurrected by continuity supplementation.
        tags, unit = metrics.get(name, ((), None))
        for hold in supplement['evidence'].get('auxiliary_holds', []):
            if hold['tag'] not in tags or hold['unit'] != unit:
                continue
            period = (sec._day(hold['start']), sec._day(hold['end']))
            fact = series.get(period)
            if fact is None or fact['filed'] <= supplement['evidence']['filed']:
                series[period] = None
        for fact in series.values():
            if fact and fact['accession'] == supplement['evidence']['accession']:
                proof=supplement['provenance'].get((fact['tag'],fact['unit'],fact['start'],fact['end']))
                if proof:
                    fact.update(proof)
    return records
