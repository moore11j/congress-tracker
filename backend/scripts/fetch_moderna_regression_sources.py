"""Fetch eight hash-pinned public regression inputs, once each, without redirects."""
from pathlib import Path
import argparse,hashlib,json,re,urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):return None

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    manifest=json.loads((Path(__file__).resolve().parents[1]/'tests/fixtures/issuer_moderna_nov25/manifest.json').read_bytes())
    files=manifest['files']
    if len(files)!=8:raise ValueError('Unexpected source inventory')
    destination=Path(args.output);destination.mkdir(parents=True,exist_ok=False)
    opener=urllib.request.build_opener(NoRedirect())
    for name,proof in files.items():
        if not re.fullmatch(r'[a-z0-9-]+\.(?:html|pdf)',name):raise ValueError('Invalid fixture name')
        url=proof['url']
        if url!='https://investors.modernatx.com/quarterly-results' and not re.fullmatch(
                r'https://irp\.cdn-website\.com/(?:89dd0539|0a71bb4f)/files/uploaded/MRNA-USQ_Transcript_\d{4}-\d{2}-\d{2}(?:-[1-9]\d?|\+%28[1-9]\d?%29)?\.pdf',url):
            raise ValueError('Unreviewed source URL')
        request=urllib.request.Request(url,headers={'User-Agent':'Walnut Markets source verification contact@walnutmarkets.com'})
        with opener.open(request,timeout=25) as response:
            if response.status!=200:raise ValueError('Source unavailable')
            raw=response.read(2_000_001)
        if len(raw)>2_000_000 or len(raw)!=proof['bytes'] or hashlib.sha256(raw).hexdigest()!=proof['sha256']:
            raise ValueError('Source changed; independent review required')
        (destination/name).write_bytes(raw)
    print(json.dumps(dict(status='complete',files=len(files))))

if __name__=='__main__':main()
