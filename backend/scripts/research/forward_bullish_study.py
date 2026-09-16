"""Append-only local forward-study runner. Remote operations are read-only exports."""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys

from forward_bullish_core import prepare_trades,new_decisions,price_books,measure,summary,stamp,REPORT_DATE,ENROLL_END
from options_activity_pilot import digest,ny_timezone,LEDGER_HASH

BASE=Path('frontend/test-results/confirmation-research')
ROOT=BASE/'forward-bullish-2026-09-15'
PLAN=ROOT/'frozen-plan-v2.json'
SCRIPTS=Path('backend/scripts/research')
PROTOCOL=Path('docs/confirmation-forward-study-protocol-2026-09-15.md')


def append(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    serialized=json.dumps(value,indent=2,allow_nan=False)
    if path.exists():
        if json.loads(path.read_text())!=value:raise ValueError('Refusing to overwrite frozen artifact: '+str(path))
        return
    with path.open('x',encoding='utf-8') as stream:stream.write(serialized)


def initialize():
    assert digest(BASE/'cohort.json')==LEDGER_HASH
    files=[SCRIPTS/n for n in ['forward_bullish_core.py','forward_bullish_study.py','export_forward_capture.py',
                               'conviction_features.py','options_activity_pilot.py','analyze_disclosure_triggers.py']]+[PROTOCOL]
    path=PLAN
    if path.exists():
        plan=json.loads(path.read_text())
        if any(digest(Path(p))!=h for p,h in plan['hashes'].items()):raise ValueError('Frozen forward-study source changed')
    else:
        plan={'initialized_at':datetime.now(timezone.utc).isoformat(),'hashes':{str(p):digest(p) for p in files},
              'ledger_sha256':LEDGER_HASH,'email_enabled':False,'enrollment_end':ENROLL_END,'report_date':REPORT_DATE}
        append(path,plan)
    return plan


def captures():
    return [json.loads(p.read_text()) for p in sorted((ROOT/'captures').glob('*.json'))]


def load_decisions():
    rows=[]
    for c in captures():rows.extend(c['decisions'])
    return rows


def capture():
    plan=initialize();now=datetime.now(timezone.utc);today=now.astimezone(ny_timezone()).date().isoformat()
    path=ROOT/'captures'/(today+'.json')
    if path.exists():print('Capture already frozen for '+today);return
    raw=ROOT/'raw'/(now.strftime('%Y%m%dT%H%M%SZ')+'.json')
    subprocess.run([sys.executable,str(SCRIPTS/'run_readonly_export.py'),str(SCRIPTS/'export_forward_capture.py'),str(raw)],check=True)
    payload=json.loads(raw.read_text());recorded=datetime.now(timezone.utc).isoformat()
    if not payload.get('read_only') or not payload['confirmations']:raise ValueError('Empty or unverified capture')
    if stamp(payload['captured_at']).astimezone(ny_timezone()).date().isoformat()!=today:
        raise ValueError('Server capture does not belong to the scheduled local day')
    prior=captures();baseline=ROOT/'baseline.json'
    ids=sorted(str(c['anchor']['id']) for c in payload['confirmations'])
    if not baseline.exists():
        append(baseline,{'captured_at':payload['captured_at'],'recorded_at':recorded,'ids':ids,'raw_path':str(raw),
                         'raw_sha256':digest(raw),'policy':'Existing calls establish baseline only; none enrolled retrospectively'})
        decisions=[];excluded={'initial_baseline_only':len(ids)};audit={}
    else:
        initial=json.loads(baseline.read_text());seen=set(initial['ids'])
        for c in prior:seen.update(c['observed_ids'])
        trades,audit=prepare_trades(payload,payload['captured_at'])
        decisions,excluded=new_decisions(payload,seen,initial['captured_at'],recorded,trades)
    # Ensure prediction files really precede the next entry; elapsed export time cannot backdate them.
    value={'captured_at':payload['captured_at'],'recorded_at':recorded,'raw_path':str(raw),'raw_sha256':digest(raw),
           'plan_sha256':digest(PLAN),'observed_ids':ids,'decisions':decisions,
           'excluded':excluded,'trade_audit':audit,'counts':{'confirmation_events':len(ids),
           'source_snapshots':payload['snapshot_rows_read'],'insider_events':len(payload['trade_inputs']['events']),
           'normalized_transactions':len(payload['trade_inputs']['normalized']),'price_rows':len(payload['prices'])},
           'latest_confirmation_at':max(c['current']['calculated_at'] for c in payload['confirmations'])}
    append(path,value)
    print(json.dumps({'capture':str(path),'new_decisions':len(decisions),'counts':value['counts'],'exclusions':excluded}))


def evaluate():
    initialize();cc=captures()
    if not cc:raise ValueError('No verified capture; cannot evaluate')
    now=datetime.now(timezone.utc);tag=now.strftime('%Y%m%dT%H%M%SZ')
    # Preserve all provider-specific observations from captured files, preferring the latest cache vintage.
    prices={};decisions=load_decisions()
    for c in cc:
        path=Path(c['raw_path'])
        if digest(path)!=c['raw_sha256']:raise ValueError('Captured raw inputs changed')
        for row in json.loads(path.read_text())['prices']:prices[(row[6],row[0],row[1])]=row
    books=price_books(prices.values());outcomes={}
    asof=cc[-1]['captured_at']
    for d in decisions:
        outcomes[d['id']]={}
        for h in [7,30]:
            path=ROOT/'measurements'/(d['id']+'_'+str(h)+'.json')
            if path.exists():result=json.loads(path.read_text())
            else:
                result=measure(d,h,books,asof)
                if result['status']=='measured':append(path,result)
            outcomes[d['id']][str(h)]=result
    first={}
    for d in sorted(decisions,key=lambda r:(r['recorded_at'],r['id'])):first.setdefault(d['ticker'],d)
    sets={'all':decisions,'bullish':[d for d in decisions if d['direction']=='bullish'],
          'bearish':[d for d in decisions if d['direction']=='bearish'],'first_per_ticker':list(first.values()),
          'known_bullish':[d for d in decisions if d['direction']=='bullish' and d['challenger']['status']!='unknown_fallback']}
    missing=[];start=stamp(cc[0]['recorded_at']).astimezone(ny_timezone()).date();end=min(now.astimezone(ny_timezone()).date()-timedelta(days=1),datetime.fromisoformat(ENROLL_END).date())
    actual={stamp(c['recorded_at']).astimezone(ny_timezone()).date() for c in cc}
    d=start
    while d<=end:
        if d.weekday()<5 and d not in actual:missing.append(d.isoformat())
        d+=timedelta(days=1)
    results={'generated_at':now.isoformat(),'latest_data_capture':asof,'decisions':len(decisions),
             'state':'final' if now.astimezone(ny_timezone()).date().isoformat()>=REPORT_DATE else 'collecting_or_maturing',
             'missing_capture_days':missing,'capture_count':len(cc),'latest_confirmation_at':cc[-1]['latest_confirmation_at'],
             'summary':{name:{str(h):{policy:summary(rows,outcomes,h,policy=='challenger') for policy in ['baseline','challenger']} for h in [7,30]} for name,rows in sets.items()},
             'outcomes':outcomes,'email_enabled':False,'plan_sha256':digest(PLAN)}
    append(ROOT/'evaluations'/(tag+'.json'),results)
    if results['state']=='final':
        lines=['# Bullish forward study: initial assessment','',
               'Separate prospective research; public scores and historical outcomes unchanged. Email remains disabled.','',
               '| Sample | Horizon | Model | Correct / measured | Accuracy | Retained coverage | Mean excess |',
               '|---|---|---|---|---|---|---|']
        for name,hh in results['summary'].items():
            for h,policies in hh.items():
                for policy,s in policies.items():
                    fmt=lambda x:'n/a' if x is None else f'{x:.2f}%'
                    lines.append(f'| {name} | {h}D | {policy} | {s["correct"]}/{s["retained"]} | {fmt(s["accuracy"])} | {fmt(s["coverage"])} | {fmt(s["average_excess"])} |')
        lines+=['','Missing capture dates: '+', '.join(missing),
                'Results are descriptive: unknown-input fallback, sparse conviction cases, shared dates/tickers and missing prices limit inference. No automatic production promotion.',
                'Detailed statuses, descriptive intervals, immutable predictions and price evidence are in the accompanying local evaluation JSON.']
        path=ROOT/'reports'/(tag+'.md');path.parent.mkdir(parents=True,exist_ok=True);path.write_text('\n'.join(lines),encoding='utf-8')
    assert digest(BASE/'cohort.json')==LEDGER_HASH
    print(json.dumps({k:results[k] for k in ['state','decisions','capture_count','missing_capture_days','latest_confirmation_at']}))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--capture',action='store_true');parser.add_argument('--evaluate',action='store_true');args=parser.parse_args()
    ROOT.mkdir(parents=True,exist_ok=True)
    import msvcrt
    with (ROOT/'runner.lock').open('a+b') as lock:
        lock.seek(0)
        if not lock.read(1):lock.write(b'0');lock.flush()
        lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
        try:
            if args.capture:capture()
            if args.evaluate:evaluate()
            if not args.capture and not args.evaluate:initialize();print('Protocol and code frozen; no capture requested')
        finally:lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_UNLCK,1)


if __name__=='__main__':main()
