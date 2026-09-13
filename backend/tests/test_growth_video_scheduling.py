from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from test_growth_video import db
from test_growth_video_automation import ready, Buffer
from app.models import UserAccount
from app.routers import growth_video as api
from app.services import growth_buffer as buffer, growth_video_store as store


def future():
    return (datetime.now(timezone.utc)+timedelta(days=1)).isoformat(timespec="milliseconds")


class ScheduledBuffer(Buffer):
    def create(self, platform, caption, url, *, scheduled_at=None):
        self.calls.append((platform,caption,url,scheduled_at))
        return {"id":platform+"-post","status":"scheduled","dueAt":scheduled_at,"schedulingType":"automatic"}


def test_schedule_is_durable_idempotent_and_handed_to_buffer_once(db, monkeypatch):
    item=ready(db,monkeypatch);client=ScheduledBuffer();due=future()
    kwargs=dict(scheduled_at=due,schedule_timezone="America/Los_Angeles",client=client)
    buffer.approve_publish(db,item,1,["instagram","tiktok"],"caption with links",**kwargs)
    buffer.approve_publish(db,store.job(db,item['id']),1,["instagram","tiktok"],"caption with links",**kwargs)
    assert not client.calls
    assert store.job(db,item['id'])['payload']['approval']['action']=='approve_and_schedule'
    # Re-running the additive migration preserves existing schedules.
    buffer.ensure_schema(db)
    assert len(buffer.publications(db,item['id']))==2
    buffer.run_pending(db,client=client)
    buffer.run_pending(db,client=client)
    assert len(client.calls)==2 and all(call[3]==due for call in client.calls)
    assert all(p['status']=='SCHEDULED' and p['scheduled_at']==due and p['buffer_due_at']==due for p in buffer.publications(db))
    assert all('token' not in p for p in buffer.publications(db))
    db.execute(text("UPDATE growth_video_publications SET checked_at='2000-01-01T00:00:00+00:00'"));db.commit()
    buffer.run_pending(db,client=client)
    assert all(p['status']=='PUBLISHED' for p in buffer.publications(db))
    assert len(client.calls)==2


@pytest.mark.parametrize('due,zone',[
    ('not-a-date','UTC'),('2030-01-01T09:00:00','UTC'),('2000-01-01T09:00:00Z','UTC'),
    ('2030-01-01T09:00:00Z','Not/AZone'),(None,'America/Los_Angeles'),
])
def test_invalid_schedules_create_no_publication(db,monkeypatch,due,zone):
    item=ready(db,monkeypatch);client=ScheduledBuffer()
    with pytest.raises(ValueError):buffer.approve_publish(db,item,1,['instagram'],'caption',scheduled_at=due,schedule_timezone=zone,client=client)
    assert not buffer.publications(db) and not client.calls
    assert store.job(db,item['id'])['status']=='READY_FOR_REVIEW'


def test_timezone_offset_is_normalized_to_utc():
    due,zone=buffer.validate_schedule('2030-07-01T09:00:00-07:00','America/Los_Angeles')
    assert due=='2030-07-01T16:00:00.000+00:00' and zone=='America/Los_Angeles'


def test_changed_schedule_cannot_create_an_extra_or_immediate_post(db,monkeypatch):
    item=ready(db,monkeypatch);due=future();client=ScheduledBuffer()
    buffer.approve_publish(db,item,1,['instagram'],'caption',scheduled_at=due,client=client)
    with pytest.raises(ValueError,match='different publishing time'):
        buffer.approve_publish(db,store.job(db,item['id']),1,['tiktok'],'caption',client=client)
    assert len(buffer.publications(db))==1 and not client.calls


def test_expired_schedule_fails_without_sending_and_requires_new_time_to_retry(db,monkeypatch):
    item=ready(db,monkeypatch);client=ScheduledBuffer()
    buffer.approve_publish(db,item,1,['instagram'],'caption',scheduled_at=future(),client=client)
    db.execute(text("UPDATE growth_video_publication_schedules SET scheduled_at='2000-01-01T00:00:00+00:00'"));db.commit()
    buffer.run_pending(db,client=client)
    assert not client.calls and buffer.publications(db)[0]['status']=='FAILED'
    with pytest.raises(HTTPException,match='10 minutes'):
        api.retry_publish(item['id'],api.RetryPublish(platform='instagram',confirmed_no_buffer_post=True),db)
    due=future()
    api.retry_publish(item['id'],api.RetryPublish(platform='instagram',confirmed_no_buffer_post=True,scheduled_at=due,schedule_timezone='UTC'),db)
    buffer.run_pending(db,client=client)
    assert client.calls[0][3]==due and buffer.publications(db)[0]['status']=='SCHEDULED'


def test_scheduling_requires_same_explicit_review_as_publish_now(db,monkeypatch):
    item=ready(db,monkeypatch)
    with pytest.raises(HTTPException,match='Review the finished video'):
        api.publish(item['id'],api.Publish(platforms=['instagram'],caption='caption',scheduled_at=future()),db.get(UserAccount,1),db)
    assert not buffer.publications(db)


def test_buffer_scheduling_payload_uses_custom_time_and_preserves_video_settings(monkeypatch):
    monkeypatch.setenv('BUFFER_API_KEY','test');client=buffer.BufferClient();calls=[]
    def query(q,variables):
        calls.append(variables['input'])
        return {'createPost':{'post':{'id':'post'}}}
    monkeypatch.setattr(client,'query',query)
    due=future();client.create('instagram','caption','https://example.com/video.mp4',scheduled_at=due)
    payload=calls[0]
    assert payload['mode']=='customScheduled' and payload['dueAt']==due
    assert payload['schedulingType']=='automatic' and payload['aiAssisted'] is True
    assert payload['metadata']['instagram']=={'isAiGenerated':True,'type':'reel','shouldShareToFeed':True}
    with pytest.raises(buffer.BufferError):client.create('instagram','caption','https://example.com/video.mp4',scheduled_at='2000-01-01T00:00:00+00:00')
    assert len(calls)==1


def test_uncertain_scheduled_submission_is_not_retried(db,monkeypatch):
    item=ready(db,monkeypatch);client=ScheduledBuffer()
    def uncertain(*args,**kwargs):client.calls.append(kwargs);raise TimeoutError()
    monkeypatch.setattr(client,'create',uncertain)
    buffer.approve_publish(db,item,1,['instagram'],'caption',scheduled_at=future(),client=client)
    buffer.run_pending(db,client=client);buffer.run_pending(db,client=client)
    assert len(client.calls)==1 and buffer.publications(db)[0]['status']=='UNCERTAIN'


def test_removed_buffer_post_is_not_still_reported_scheduled(db,monkeypatch):
    item=ready(db,monkeypatch);client=ScheduledBuffer()
    buffer.approve_publish(db,item,1,['instagram'],'caption',scheduled_at=future(),client=client)
    buffer.run_pending(db,client=client)
    db.execute(text("UPDATE growth_video_publications SET checked_at=NULL"));db.commit()
    monkeypatch.setattr(client,'post',lambda _:None)
    buffer.run_pending(db,client=client)
    assert buffer.publications(db)[0]['status']=='CANCELLED' and len(client.calls)==1


def test_stale_preflight_cannot_overwrite_a_submitted_schedule(db,monkeypatch):
    item=ready(db,monkeypatch);client=ScheduledBuffer()
    buffer.approve_publish(db,item,1,['instagram'],'caption',scheduled_at=future(),client=client)
    stale=buffer.publications(db)[0]
    buffer.run_pending(db,client=client)
    buffer.update(db,stale,'FAILED',error='Old worker')
    assert buffer.publications(db)[0]['status']=='SCHEDULED'


def test_paused_channel_cannot_accept_a_schedule(db,monkeypatch):
    item=ready(db,monkeypatch);client=ScheduledBuffer()
    monkeypatch.setattr(client,'channel',lambda _: {'isQueuePaused':True})
    with pytest.raises(buffer.BufferError,match='paused'):
        buffer.approve_publish(db,item,1,['instagram'],'caption',scheduled_at=future(),client=client)
    assert not buffer.publications(db)
