import copy
import json
import pytest
from app.services import walnut_takes as takes


def articles(count=8):
    return [{'title':f'Issuer {i} reports an operating update', 'url':f'https://example.com/{i}/'+('long-source-path-'*35),
             'summary':'An issuer reported an operating update.', 'source':'finnhub', 'image':f'https://example.com/image/{i}',
             'published_at':'2026-10-11T20:00:00+00:00'} for i in range(count)]


def output(key='0', **kwargs):
    return {'id':key,'bias':'neutral','take':'The filing adds context for the next report.',**kwargs}


@pytest.fixture
def transport(monkeypatch):
    calls=[]; costs=[]
    monkeypatch.setattr(takes,'_walnut_take_model',lambda db:'gpt-5.4-nano')
    monkeypatch.setattr(takes,'resolved_setting_value',lambda *a:'fixture')
    monkeypatch.setattr(takes,'_record_openai_usage_cost',lambda *a,**k:costs.append(k))
    def install(transform=None,status='completed'):
        def send(**kwargs):
            payload=kwargs['payload'];calls.append(payload)
            source=json.loads(payload['input'].split('Articles:\n',1)[1])
            rows=[output(item['id']) for item in reversed(source)]
            if transform is not None: rows=transform(rows)
            class Response:
                status_code=200
                def json(self):
                    return {'status':status,'incomplete_details':{'reason':'max_output_tokens'} if status=='incomplete' else None,
                            'output_text':json.dumps({'items':rows}),'usage':{'output_tokens':300}}
            return Response()
        monkeypatch.setattr(takes,'audited_openai_request',send)
    install()
    return install,calls,costs


def test_long_urls_use_short_ids_and_restore_exact_cache_identity(transport):
    _,calls,costs=transport
    source=articles();before=copy.deepcopy(source)
    result=takes._generate_openai_takes(None,api_key='fixture',articles=source)
    request=json.loads(calls[0]['input'].split('Articles:\n',1)[1])
    assert [r['id'] for r in request]==[str(i) for i in range(8)]
    assert [r['article_url'] for r in request]==[r['url'] for r in source]
    assert [r['id'] for r in result]==[takes._article_id(r) for r in reversed(source)]
    assert source==before and calls[0]['max_output_tokens']==720
    assert calls[0]['model']=='gpt-5.4-nano' and 'tools' not in calls[0]
    assert costs[0]['web_search_calls']==0
    old_json=json.dumps({'items':[output(takes._article_id(r)) for r in source]})
    new_json=json.dumps({'items':[output(str(i)) for i in range(8)]})
    assert len(new_json)<len(old_json)/3


def test_reordered_multi_batch_responses_keep_article_provenance(transport):
    install,calls,_=transport
    def decorate(rows):
        batch=len(calls)-1
        return [{**r,'take':f'Issuer {batch*8+int(r["id"])} adds context for the next report.'} for r in rows]
    install(decorate)
    source=articles(11)
    result=takes.enrich_walnut_takes(None,source)
    assert [p['max_output_tokens'] for p in calls]==[720,270]
    for index,(original,enriched) in enumerate(zip(source,result)):
        assert all(enriched[k]==v for k,v in original.items())
        assert enriched['walnut_take']==f'Issuer {index} adds context for the next report.'
        assert enriched['walnut_take_source']=='openai'
    before=len(calls)
    repeated=takes.enrich_walnut_takes(None,source,previous_items=result)
    assert repeated==result and len(calls)==before


@pytest.mark.parametrize('transform',[
    lambda rows:rows[:-1], lambda rows:rows+[rows[0]], lambda rows:[output('999')],
    lambda rows:[output(0)], lambda rows:[output('0',bias='unknown')],
    lambda rows:[output('0',take='')], lambda rows:[None],
    lambda rows:[output('url:https://example.com/0')]])
def test_bad_article_mapping_never_gets_a_generated_label(transport,transform):
    install,_,_=transport; install(transform)
    source=articles(2)
    with pytest.raises(ValueError): takes._generate_openai_takes(None,api_key='fixture',articles=source)
    result=takes.enrich_walnut_takes(None,source)
    assert len(result)==2 and all(r['walnut_take_source']=='fallback' for r in result)
    assert [r['url'] for r in result]==[r['url'] for r in source]


def test_incomplete_status_rejects_even_parseable_json(transport):
    install,_,_=transport;install(status='incomplete')
    with pytest.raises(ValueError,match='max_output_tokens'):
        takes._generate_openai_takes(None,api_key='fixture',articles=articles(1))
