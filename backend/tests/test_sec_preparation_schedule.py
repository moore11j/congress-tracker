"""Shared SEC collectors must receive distinct recurring start opportunities."""
from pathlib import Path


def _minutes(field):
    values=set()
    for part in field.split(','):
        interval,_,step=part.partition('/')
        step=int(step or '1')
        if interval=='*':first,last=0,59
        elif '-' in interval:first,last=map(int,interval.split('-'))
        else:first=last=int(interval)
        values.update(range(first,last+1,step))
    return values


def test_five_minute_sec_collectors_do_not_start_together():
    jobs={}
    for line in (Path(__file__).parents[1]/'crontab').read_text().splitlines():
        if line.startswith('#') or 'python -m app.jobs.' not in line:continue
        name=line.split('python -m app.jobs.',1)[1].split()[0]
        if name.startswith('warm_sec_') or name=='collect_direct_13f_priors':
            fields=line.split()
            assert fields[1:5]==['*']*4
            assert name not in jobs
            jobs[name]=_minutes(fields[0])
    assert set(jobs)=={'warm_sec_research','warm_sec_metadata','warm_sec_fundamentals',
                       'warm_sec_earnings','collect_direct_13f_priors'}
    occupied=set()
    for name,minutes in jobs.items():
        assert len(minutes)==12
        assert not occupied & minutes, f'{name} collides with another SEC collector'
        occupied.update(minutes)
    assert occupied==set(range(60))
