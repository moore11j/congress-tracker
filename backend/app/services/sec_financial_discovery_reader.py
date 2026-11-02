"""Qualify one discovered filing using the existing three financial readers.

No storage, scheduling or publication. A qualified filing is still not proof
that it is the current statement: the caller must reconcile the whole scan.
"""
import hashlib
import json
import os
import re
from copy import deepcopy
from datetime import date,datetime
from lxml import html
from app.services import sec_financial_discovery as discovery

VERSION='sec_financial_discovery_qualification_v5'
MAX_ORIGINALS=6


def prepared_choice(company_raw,facts_raw,*,symbol,cik,available_by,observed_at,client):
    """Opt-in existing-reader integration; page requests never collect sources."""
    if os.getenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED','0')!='1':return None
    from app.request_priority import get_request_context
    if get_request_context():
        return None
    from app.services import sec_financial_discovery_store as store
    try:
        checkpoint=store.read(symbol,cik)
        if checkpoint is None:return None
        return restore(company_raw,facts_raw,symbol=symbol,cik=cik,available_by=available_by,
            observed_at=observed_at,checkpoint=checkpoint,client=client)
    except discovery.DiscoveryError as exc:
        # Existing financial consumers already handle source-qualification
        # errors. A pending scan must not become an uncaught page exception.
        from app.services.sec_tabular_reporting import TabularReportingError
        reason='financial_discovery_pending' if str(exc)=='discovery_incomplete' else 'financial_discovery_held'
        raise TabularReportingError(reason) from exc


def selection_plan(company_raw,*,symbol,cik,available_by,observed_at,checkpoint):
    """Select from complete qualified inventory, retaining unresolved newer risk."""
    plan=discovery.inspect(company_raw,symbol=symbol,cik=cik,available_by=available_by,
                           observed_at=observed_at,previous=checkpoint)
    receipts=list(plan['state']['receipts'].values())
    pending=sorted((r for r in receipts if r['status']=='observed' and 'qualification' not in r),
                   key=lambda r:(r['candidate']['filed'],r['candidate']['accession']),reverse=True)
    result={'status':'pending','next_accession':pending[0]['candidate']['accession'] if pending else None,
        'pending_covers':plan['remaining'],'pending_qualifications':len(pending),'selected_accessions':[],
        'financial_publication_eligible':False,'complete_universe_coverage':False,'reason':None}
    if plan['remaining'] or pending:return result
    qualified=[r['qualification'] for r in receipts if r.get('qualification',{}).get('status')=='qualified']
    if not qualified:
        return {**result,'status':'held','reason':'no_qualified_statement'}
    rank=lambda q:(q['current_period'],q['candidate']['filed'])
    newest=max(map(rank,qualified));selected=[q for q in qualified if rank(q)==newest]
    # A filing date is not a statement period. An unresolved later disclosure
    # can contain a more recent quarter even when its directory date is an event.
    held=[r for r in receipts if r['status']!='observed' or r.get('qualification',{}).get('status')=='held']
    if any(r['candidate']['filed']>newest[0] for r in held):
        return {**result,'status':'held','reason':'discovery_has_unqualified_statement'}
    from app.services.sec_reporting_policy import DOMESTIC_FORMS
    recent=json.loads(company_raw)['filings']['recent']
    for form,end,filed in zip(recent['form'],recent['reportDate'],recent['filingDate']):
        try:period,filing=date.fromisoformat(end),date.fromisoformat(filed)
        except (ValueError,TypeError):continue
        if form in DOMESTIC_FORMS and period<=filing<=available_by and period>=date.fromisoformat(newest[0]):
            return {**result,'status':'held','reason':'discovery_domestic_reporting_boundary'}
    if len(selected)>3 or (len(selected)>1 and any(q['kind']!='html_table' for q in selected)):
        return {**result,'status':'held','reason':'discovery_competing_statements'}
    return {**result,'status':'ready_for_original_revalidation',
            'selected_accessions':sorted(q['candidate']['accession'] for q in selected)}


def validate_receipt(receipt,cover,*,now):
    """Validate compact persisted qualification, never substitute it for facts."""
    try:
        if not isinstance(receipt,dict) or len(discovery._json(receipt).encode())>16_384:
            raise ValueError('shape')
        if not isinstance(receipt.get('version'),str):raise ValueError('version')
        if receipt['version']!=VERSION:return None
        stamp=datetime.fromisoformat(receipt['observed_at'])
        cover_stamp=datetime.fromisoformat(cover['observed_at'])
        status=receipt['status'];originals=receipt['originals'];holds=receipt['holds']
        if (receipt['candidate']!=cover['candidate'] or receipt['cover_sha256']!=cover['cover_sha256']
                or receipt['financial_publication_eligible'] is not False
                or stamp.tzinfo is None or not cover_stamp<=stamp<=now
                or status not in {'qualified','held','no_supported_statement'}
                or not isinstance(originals,list) or not 1<=len(originals)<=MAX_ORIGINALS
                or not isinstance(holds,list) or len(holds)>16):raise ValueError('identity')
        urls=set()
        for original in originals:
            url=original['url']
            if (not discovery.valid_document_proof(original) or not discovery._link_in_scope(url,cover['candidate']) or url in urls
                    or not re.fullmatch(r'[0-9a-f]{64}',original['sha256'])
                    or type(original['bytes']) is not int or not 0<original['bytes']<=20_000_000):
                raise ValueError('original')
            urls.add(url)
        original=next((r for r in originals if r['url']==cover['candidate']['cover_url']),None)
        if original is None or ((original['sha256']!=cover['cover_sha256'] or original['bytes']!=cover['bytes'])
                and not discovery.same_document(original,cover)):
            raise ValueError('cover')
        for hold in holds:
            if (not isinstance(hold,dict) or hold.get('accession')!=cover['candidate']['accession']
                    or not isinstance(hold.get('reason'),str) or not 0<len(hold['reason'])<=300):
                raise ValueError('hold')
        if status=='qualified':
            if (receipt['kind'] not in {'standard_inline','native_inline','html_table'}
                    or holds
                    or not date.fromisoformat(receipt['current_period'])<=date.fromisoformat(cover['candidate']['filed'])):
                raise ValueError('qualified')
        elif receipt['kind'] is not None or receipt['current_period'] is not None:
            raise ValueError('unqualified')
        if status=='held' and not holds:raise ValueError('missing_hold')
        if status=='no_supported_statement' and holds:raise ValueError('hidden_hold')
        if status=='no_supported_statement' and (cover['primary_statement']
                or any(k!='press_release' for k in cover['link_kinds'].values())):
            raise ValueError('unqualified_financial_hint')
        # Retry only legacy accounting-basis holds. Existing successful receipts
        # retain exact comparison semantics and do not cause a full source reset.
        if status=='held' and any(h['reason']=='table_accounting_basis_unproved' for h in holds):
            from app.services.sec_statement_declarations import POLICY
            if receipt.get('accounting_policy')!=POLICY:return None
            from app.services.sec_comparative_basis import POLICY as comparative_policy
            if receipt.get('comparative_policy')!=comparative_policy:return None
        if status=='held':
            from app.services.sec_statement_sections import POLICY as section_policy, RETRY_REASONS
            if any(h['reason'] in RETRY_REASONS for h in holds) and receipt.get('statement_section_policy')!=section_policy:
                return None
        if status=='held' and any(h['reason']=='financial_hint_not_qualified' for h in holds):
            from app.services.sec_statement_heading_order import POLICY as heading_policy
            if receipt.get('statement_heading_policy')!=heading_policy:return None
        if status=='held':
            from app.services.sec_statement_layout import POLICY as layout_policy, RETRY_REASONS as layout_reasons
            if any(h['reason'] in layout_reasons for h in holds) and receipt.get('statement_layout_policy')!=layout_policy:
                return None
        return deepcopy(receipt)
    except (ValueError,TypeError,KeyError,AttributeError) as exc:
        raise discovery.DiscoveryError('discovery_qualification_invalid') from exc


def restore(company_raw,facts_raw,*,symbol,cik,available_by,observed_at,checkpoint,client,annual_rollover=False):
    """Re-fetch and revalidate selected complete originals, never stored values."""
    common=dict(symbol=symbol,cik=cik,available_by=available_by,observed_at=observed_at)
    plan=selection_plan(company_raw,checkpoint=checkpoint,**common)
    if plan['status']!='ready_for_original_revalidation':
        raise discovery.DiscoveryError(plan['reason'] or 'discovery_incomplete')
    validated=discovery.inspect(company_raw,previous=checkpoint,**common)
    selected=plan['selected_accessions']
    qualifications={r['candidate']['accession']:r['qualification']
        for r in validated['state']['receipts'].values() if r['candidate']['accession'] in selected}
    expected={r['url']:r for q in qualifications.values() for r in q['originals']}
    if len(expected)>MAX_ORIGINALS:raise discovery.DiscoveryError('discovery_original_bound')
    originals={}
    class VerifiedOriginals:
        def get(self,url):
            if url not in expected:raise discovery.DiscoveryError('discovery_unqualified_original')
            if url not in originals:
                raw=client.get(url);proof=expected[url]
                try:
                    from app.services.sec_original_integrity import verified_original
                    verified_original(raw,proof)
                    if hashlib.sha256(raw).hexdigest()==proof['sha256'] and len(raw)!=proof['bytes']:
                        raise ValueError('original_size')
                except (ValueError,TypeError):
                    raise discovery.DiscoveryError('discovery_qualified_original_changed')
                originals[url]=raw
            return originals[url]
    retained=VerifiedOriginals()
    if len(selected)==1:
        result=inspect_candidate(company_raw,facts_raw,checkpoint=validated['state'],
                                 accession=selected[0],client=retained,annual_rollover=annual_rollover,**common)
        expected_receipt=deepcopy(qualifications[selected[0]])
        expected_receipt['observed_at']=observed_at.isoformat()
        comparison=deepcopy(result['receipt'])
        # Only proven terminal delivery-path changes may differ on re-fetch.
        # Return the actual fetched bytes/provenance in the supplement.
        pinned={r['url']:r for r in expected_receipt['originals']}
        for original in comparison['originals']:
            prior=pinned.get(original['url'])
            if prior and discovery.same_document(original,prior):
                original['sha256']=prior['sha256'];original['bytes']=prior['bytes']
        if comparison!=expected_receipt:
            raise discovery.DiscoveryError('discovery_qualification_changed')
        supplement=result['supplement']
    else:
        if str(json.loads(facts_raw).get('cik','')).zfill(10)!=str(cik).zfill(10):
            raise discovery.DiscoveryError('discovery_facts_identity')
        from app.services import sec_tabular_reporting as tables
        supplement=tables.acquire(company_raw,symbol=symbol,cik=cik,available_by=available_by,
                                   client=retained,candidate_accessions=tuple(selected),annual_rollover=annual_rollover,basis_facts_raw=facts_raw)
        if supplement is None:raise discovery.DiscoveryError('discovery_qualification_changed')
        tables.validate(supplement,company_raw=company_raw,symbol=symbol,cik=cik,available_by=available_by)
    if set(originals)!=set(expected):raise discovery.DiscoveryError('discovery_originals_not_revalidated')
    return supplement


def inspect_candidate(company_raw,facts_raw,*,symbol,cik,available_by,observed_at,
                      checkpoint,accession,client,annual_rollover=False):
    from app.services import sec_document_sets as sets,sec_inline_financials as inline
    from app.services import sec_tagged_foreign as standard,sec_statement_currency as native
    from app.services import sec_tabular_reporting as tables
    from app.services.sec_reporting_policy import reviewed
    from app.services.sec_currency_ratios import basis
    from app.services.sec_foreign_statements import profile
    plan=discovery.inspect(company_raw,symbol=symbol,cik=cik,available_by=available_by,
                           observed_at=observed_at,previous=checkpoint)
    candidates=sets.candidates(company_raw,symbol=symbol,cik=cik,available_by=available_by)
    candidate=sets.bounded_candidates(candidates,(accession,))[0]
    identity={k:candidate[k] for k in discovery.IDENTITY_FIELDS}
    hint=plan['state']['receipts'].get(discovery._fingerprint(identity))
    if hint is None:raise discovery.DiscoveryError('discovery_cover_not_observed')
    if hint['status']!='observed':raise discovery.DiscoveryError('discovery_cover_held')
    # Preserve explicitly reviewed issuer policies and domestic boundaries.
    annual_profile=profile(symbol,cik)
    if reviewed(symbol,cik) or basis(symbol,cik) or (annual_profile and not annual_rollover):
        raise discovery.DiscoveryError('discovery_reviewed_policy_required')
    company=json.loads(company_raw);payload=json.loads(facts_raw)
    if not isinstance(payload,dict) or str(payload.get('cik','')).zfill(10)!=str(cik).zfill(10):
        raise discovery.DiscoveryError('discovery_facts_identity')
    originals={}
    class Originals:
        def get(self,url):
            if not discovery._link_in_scope(url,identity):
                raise discovery.DiscoveryError('discovery_original_outside_filing')
            if url not in originals:
                if len(originals)>=MAX_ORIGINALS:
                    raise discovery.DiscoveryError('discovery_original_bound')
                raw=client.get(url)
                if not isinstance(raw,bytes) or not raw or len(raw)>20_000_000:
                    raise discovery.DiscoveryError('discovery_original_size')
                originals[url]=raw
            return originals[url]
    retained=Originals()
    cover=retained.get(candidate['cover_url'])
    try:
        from app.services.sec_original_integrity import verified_original
        verified_original(cover,{**hint,'sha256':hint['cover_sha256']})
    except ValueError:
        # Legacy observations have no document-level pin. Re-observe those in
        # a separate worker phase; never grandfather a known content change.
        reason='discovery_cover_document_changed' if hint.get('document_sha256') else 'discovery_cover_changed'
        raise discovery.DiscoveryError(reason)
    if not hint.get('document_sha256') and discovery.document_proof(cover):
        # Upgrade raw-identical legacy covers too, before recording a new
        # qualification that must later survive normal delivery-path drift.
        raise discovery.DiscoveryError('discovery_cover_changed')
    # Route by actual source markup. Plain HTML statements must not be sent
    # through the inline-XBRL combined-document parser just because companyfacts
    # contains an older tagged report. Numeric inline facts, including custom
    # concepts, keep the existing inline qualification path and its holds.
    format_sources=[cover]
    if len(hint['links'])==1:
        format_sources.append(retained.get(hint['links'][0]))
    has_inline=False
    for raw in format_sources:
        try:tree=html.fromstring(raw)
        except (ValueError,html.etree.ParserError) as exc:
            raise discovery.DiscoveryError('discovery_original_html') from exc
        if tree.xpath('//base'):raise discovery.DiscoveryError('discovery_original_base')
        for node in tree.xpath('//script|//style|//template|//blockquote'):node.drop_tree()
        has_inline=has_inline or any(n.tag=='ix:nonfraction' for n in tree.iter())
    receipt={'version':VERSION,'candidate':identity,'cover_sha256':hint['cover_sha256'],
        'observed_at':observed_at.isoformat(),'status':'no_supported_statement',
        'financial_publication_eligible':False,'kind':None,'current_period':None,
        'holds':[],'originals':[]}
    supplement=None;scope=(accession,);currency_hold=None
    try:
        # Annual-profile continuation supports independently qualified HTML
        # only. Existing inline/issuer policies still require their own review.
        if annual_profile and has_inline:
            raise tables.TabularReportingError('discovery_reviewed_inline_policy_required')
        if has_inline:
            try:
                supplement=standard._document_set(payload,company,company_raw,symbol=symbol,cik=cik,
                    available_by=available_by,client=retained,candidate_accessions=scope)
            except standard.TaggedCurrencyError as exc:
                currency_hold=exc
        if supplement is not None:
            receipt['kind']='standard_inline'
        else:
            if has_inline:
                supplement=native.acquire(payload,company_raw,symbol=symbol,cik=cik,
                    available_by=available_by,client=retained,candidate_accessions=scope)
            if supplement is not None:receipt['kind']='native_inline'
            elif currency_hold is not None:raise currency_hold
            else:
                inspection={}
                supplement=tables.acquire(company_raw,symbol=symbol,cik=cik,available_by=available_by,
                    client=retained,candidate_accessions=scope,inspection=inspection,annual_rollover=annual_rollover,basis_facts_raw=facts_raw)
                receipt['holds']=inspection.get('holds',[])
                if supplement is not None:receipt['kind']='html_table'
        if supplement is not None:
            proof=supplement['evidence']
            if receipt['kind']=='html_table':
                tables.validate(supplement,company_raw=company_raw,symbol=symbol,cik=cik,available_by=available_by)
                source=proof['table_reporting']['source']
                period=proof['table_reporting']['current_period']
                if source['accession']!=accession:raise discovery.DiscoveryError('discovery_statement_identity')
            else:
                if proof['accession']!=accession:raise discovery.DiscoveryError('discovery_statement_identity')
                period=proof['report_date']
                if receipt['kind']=='standard_inline':
                    standard.authorized(payload,company,symbol=symbol,cik=cik,available_by=available_by,supplement=supplement)
                else:
                    native.authorized(payload,company,symbol=symbol,cik=cik,available_by=available_by,supplement=supplement)
            receipt.update(status='qualified',current_period=period)
        elif receipt['holds']:
            receipt['status']='held'
        elif hint['primary_statement'] or any(k!='press_release' for k in hint['link_kinds'].values()):
            receipt.update(status='held',holds=[{'accession':accession,'filed':candidate['filed'],
                'reason':'financial_hint_not_qualified'}])
    except discovery.DiscoveryError:
        raise
    except (inline.InlineFactsError,standard.TaggedForeignError,native.StatementCurrencyError,tables.TabularReportingError) as exc:
        receipt.update(status='held',kind=None,current_period=None,
                       holds=[{'accession':accession,'filed':candidate['filed'],'reason':str(exc)}])
        supplement=None
    receipt['originals']=[{'url':url,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),**discovery.document_proof(raw)}
                           for url,raw in sorted(originals.items())]
    if receipt['status']=='held' and any(h['reason']=='table_accounting_basis_unproved' for h in receipt['holds']):
        from app.services.sec_statement_declarations import POLICY
        receipt['accounting_policy']=POLICY
        from app.services.sec_comparative_basis import POLICY as comparative_policy
        receipt['comparative_policy']=comparative_policy
    if receipt['status']=='held':
        from app.services.sec_statement_sections import POLICY as section_policy, RETRY_REASONS
        if any(h['reason'] in RETRY_REASONS for h in receipt['holds']):
            receipt['statement_section_policy']=section_policy
    if receipt['status']=='held' and any(h['reason']=='financial_hint_not_qualified' for h in receipt['holds']):
        from app.services.sec_statement_heading_order import POLICY as heading_policy
        receipt['statement_heading_policy']=heading_policy
    if receipt['status']=='held':
        from app.services.sec_statement_layout import POLICY as layout_policy, RETRY_REASONS as layout_reasons
        if any(h['reason'] in layout_reasons for h in receipt['holds']):
            receipt['statement_layout_policy']=layout_policy
    receipt=validate_receipt(receipt,hint,now=observed_at)
    return {'receipt':receipt,'supplement':supplement}
