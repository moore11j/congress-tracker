"""Execute the actual candidate shell against fake Fly, with no network."""
import json,os,subprocess
from pathlib import Path
import pytest

SCRIPT=Path(__file__).resolve().parents[1]/'scripts/deploy_backend_guarded.sh'

@pytest.mark.parametrize('failure,expected',[
 ('none',['build','reserve','deploy','status','release']),
 ('build',['build']),
 ('reserve',['build','reserve']),
 ('deploy',['build','reserve','deploy']),
 ('status',['build','reserve','deploy','status','release']),
 ('release',['build','reserve','deploy','status','release']),
])
def test_release_sequence_and_fail_closed(tmp_path,failure,expected):
    fake=tmp_path/'flyctl'
    fake.write_text('''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
args=sys.argv[1:]
kind=('build' if '--build-only' in args else 'deploy') if args[0]=='deploy' else 'status' if args[0]=='status' else 'release' if ' release-reservation ' in args[-1] else 'reserve'
with Path(os.environ['TRACE']).open('a') as stream:stream.write(json.dumps({'kind':kind,'args':args})+'\\n')
raise SystemExit(1 if os.environ['FAILURE']==kind else 0)
''')
    fake.chmod(0o755)
    trace=tmp_path/'trace'
    env={**os.environ,'PATH':str(tmp_path)+os.pathsep+os.environ['PATH'],
         'TRACE':str(trace),'FAILURE':failure,'GITHUB_RUN_ID':'123456','GITHUB_RUN_ATTEMPT':'2','GITHUB_SHA':'a'*40}
    run=subprocess.run(['bash',str(SCRIPT)],env=env,text=True,capture_output=True,timeout=15)
    rows=[json.loads(line) for line in trace.read_text().splitlines()]
    assert [r['kind'] for r in rows]==expected,run.stderr
    assert (run.returncode==0)==(failure=='none'),run.stdout+run.stderr
    for row in rows:
        if row['kind']=='build':assert '--push' in row['args']
        if row['kind']=='reserve':assert row['args'][-1].endswith('--owner 123456-2 --seconds 900')
        if row['kind']=='deploy':assert 'registry.fly.io/congress-tracker-api:release-'+('a'*40)+'-123456-2' in row['args']
        if row['kind']=='release':assert row['args'][-1].endswith('--owner 123456-2')


def test_bad_identity_never_calls_fly(tmp_path):
    fake=tmp_path/'flyctl'
    fake.write_text('#!/bin/sh\nexit 99\n');fake.chmod(0o755)
    env={**os.environ,'PATH':str(tmp_path)+os.pathsep+os.environ['PATH'],
         'GITHUB_RUN_ID':'not-a-run','GITHUB_RUN_ATTEMPT':'2','GITHUB_SHA':'a'*40}
    run=subprocess.run(['bash',str(SCRIPT)],env=env,text=True,capture_output=True,timeout=10)
    assert run.returncode==2 and 'Invalid release identity' in run.stderr
