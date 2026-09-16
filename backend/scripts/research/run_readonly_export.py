"""Transport a reviewed read-only research script to the existing API machine."""
import base64
import json
import pathlib
import subprocess
import sys
import zlib

script, output = map(pathlib.Path, sys.argv[1:3])
encoded = base64.b64encode(zlib.compress(script.read_bytes())).decode()
command = "python -c \"import base64,zlib;exec(zlib.decompress(base64.b64decode('" + encoded + "')))\""
result = subprocess.run([str(pathlib.Path.home()/'.fly/bin/flyctl.exe'), 'ssh','console','-q','-a','congress-tracker-api','--machine','863254be261935','-C',command], capture_output=True, text=True, timeout=180)
output.parent.mkdir(parents=True, exist_ok=True)
output.with_suffix('.log').write_text(result.stdout + result.stderr, encoding='utf-8')
for line in result.stdout.splitlines():
    if line.startswith('AUDIT_JSON='):
        data=json.loads(line.split('=',1)[1]); output.write_text(json.dumps(data,indent=2),encoding='utf-8'); print(json.dumps(data,indent=2)); break
    if line.startswith('RESEARCH_BASE64='):
        raw=zlib.decompress(base64.b64decode(line.split('=',1)[1]));output.write_bytes(raw);print(f'Exported {len(raw):,} bytes to {output}');break
else:
    print((result.stdout+result.stderr)[-4000:]);raise SystemExit('No research payload returned')
