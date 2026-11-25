"""Fetch nine hash-pinned public regression inputs, once each, without redirects."""
from pathlib import Path
import argparse,hashlib,json,re,urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):return None

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    manifest=json.loads((Path(__file__).resolve().parents[1]/'tests/fixtures/issuer_nvidia_nov25/manifest.json').read_bytes())
    files=manifest['files']
    if len(files)!=9:raise ValueError('Unexpected source inventory')
    destination=Path(args.output);destination.mkdir(parents=True,exist_ok=False)
    opener=urllib.request.build_opener(NoRedirect())
    for name,proof in files.items():
        if not re.fullmatch(r'[a-z0-9-]+\.(?:json|pdf)',name):raise ValueError('Invalid fixture name')
        url=proof['url']
        from app.services import issuer_nvidia_discovery as policy
        company=dict(symbol='NVDA',investor_website=policy.BASE+'/',discovery={'kind':policy.KIND})
        if name.endswith('.json'):
            if url not in {policy.index_url(company,y) for y in (2026,2027)}:raise ValueError('Unreviewed index URL')
        else:
            key=proof['expected']['key'].split(':')
            policy.source_format(url,int(key[2]),int(key[3][1:]))
        request=urllib.request.Request(url,headers={'User-Agent':'Walnut Markets source verification contact@walnutmarkets.com'})
        with opener.open(request,timeout=25) as response:
            if response.status!=200:raise ValueError('Source unavailable')
            raw=response.read(2_000_001)
        if len(raw)>2_000_000 or len(raw)!=proof['bytes'] or hashlib.sha256(raw).hexdigest()!=proof['sha256']:
            raise ValueError('Source changed; independent review required')
        (destination/name).write_bytes(raw)
    print(json.dumps(dict(status='complete',files=len(files))))

if __name__=='__main__':main()
