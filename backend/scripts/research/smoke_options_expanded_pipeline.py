"""Exercise the complete analysis/report path using clearly synthetic temporary data."""
from contextlib import redirect_stdout
from datetime import date,timedelta
import io
import json
from pathlib import Path
import tempfile
from unittest.mock import patch

import analyze_options_activity_expanded as analysis
import report_options_activity_expanded as report
from options_activity_pilot import digest,save


def main():
    parent=Path('frontend/test-results/confirmation-research')
    with tempfile.TemporaryDirectory(prefix='synthetic-options-smoke-',dir=parent) as td:
        base=Path(td);root=base/'expanded';root.mkdir();rows=[]
        for split,start,groups in [('train','2025-05-05',10),('validation','2026-01-05',2),('test','2026-05-04',3)]:
            for group in range(groups):
                entry=(date.fromisoformat(start)+timedelta(days=7*group)).isoformat()
                for ticker in range(12):
                    correct=(ticker+group)%3!=0
                    outcomes={str(h):{'status':'measured','target_date':(date.fromisoformat(entry)+timedelta(days=h)).isoformat(),
                                     'correct':correct,'raw_correct':correct,'raw_return':2 if correct else -2,'spy_return':0} for h in [7,30]}
                    rows.append({'id':split+'_'+str(group)+'_'+str(ticker),'ticker':'SYNTH'+str(ticker),
                                 'entry_date':entry,'anchor':start,'split':split,'outcomes':outcomes,
                                 'features':{'stock:momentum_20':ticker/10,'market:momentum_20':group/20,
                                             'options:activity':(ticker+group)/10,'options:always_missing':None}})
        # An unmatured row still receives a prediction and is ranked before measurement.
        rows.append({**rows[-1],'id':'test_unmatured','ticker':'SYNTH_NEW',
                     'outcomes':{'30':{'status':'immature'},'7':{'status':'immature'}}})
        original=[{**rows[-2],'direction':'bullish'},{**rows[-3],'direction':'bearish'}]
        save(base/'cohort.json',{'events':original});ledger_hash=digest(base/'cohort.json')
        save(root/'panel.json',{'rows':rows});save(root/'features.json',{'synthetic':True})
        save(root/'cohort.json',{'symbols':['SYNTH'+str(i) for i in range(12)]})
        save(root/'analysis-plan.json',{'source_hashes':{str(Path(analysis.__file__)):digest(Path(analysis.__file__))}})
        save(root/'ledger-features.json',{'rows':[{**r,'audit':{'status':'ok'}} for r in original]})
        output=io.StringIO()
        with patch.multiple(analysis,ROOT=root,BASE=base,LEDGER_HASH=ledger_hash),redirect_stdout(output):analysis.main()
        result=json.loads((root/'results.json').read_text())
        assert result['status']=='complete'
        assert result['sizes']=={'train':120,'validation':24,'test':36}
        for family in result['test']:
            assert result['test'][family]['probabilities']['n']==36
        predictions=json.loads((root/'test-predictions.json').read_text())
        assert all('test_unmatured' in p for p in predictions['predictions'].values())
        assert digest(base/'cohort.json')==ledger_hash
        save(root/'panel-audit.json',{'planned':181,'feature_coverage':{'ok':181},'model_rows':result['sizes'],
                                     'labels':{'30':{'measured':180,'immature':1},'7':{'measured':180,'immature':1}}})
        save(root/'input-audit.json',{'passed':True,'cached_requests':0,'unique_contracts':0,'daily_bars':0,
                                     'decision_replays':0,'groups':0,'ledger_sha256':ledger_hash})
        with patch.multiple(report,ROOT=root,REPORT=base/'synthetic-report.md'),redirect_stdout(output):report.main()
        rendered=(base/'synthetic-report.md').read_text(encoding='utf-8')
        assert 'Primary result' in rendered and 'No public event was removed or rescored' in rendered
    print('PASS: synthetic end-to-end fit, selection, unmatured predictions, ledger preservation, and report generation. No real test outcomes read.')


if __name__=='__main__':main()
