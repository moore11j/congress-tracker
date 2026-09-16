"""Fetch public CFTC annual archives into local research storage only."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.request import Request,urlopen
import json
import zipfile
import io
BASE=Path('frontend/test-results/confirmation-research/cot');BASE.mkdir(exist_ok=True)
def fetch(year):
    path=BASE/f'fut_fin_txt_{year}.zip'
    url=f'https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip'
    try:
        if not path.exists():
            with urlopen(Request(url,headers={'User-Agent':'Mozilla/5.0'}),timeout=35) as r:data=r.read()
            with zipfile.ZipFile(io.BytesIO(data)) as z:z.testzip()
            path.write_bytes(data)
        return {'year':year,'url':url,'bytes':path.stat().st_size,'status':'ok'}
    except Exception as e:return {'year':year,'url':url,'status':str(e)}
with ThreadPoolExecutor(max_workers=3) as pool:
    results=list(pool.map(fetch,range(2013,2027)))
(BASE/'download-manifest.json').write_text(json.dumps(results,indent=2))
print(json.dumps(results,indent=2))
