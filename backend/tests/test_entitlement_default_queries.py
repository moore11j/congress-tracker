from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker

from app import entitlements as e
from app.models import FeatureGate, PlanLimit


def factory():
    engine=create_engine('sqlite://')
    FeatureGate.__table__.create(engine)
    PlanLimit.__table__.create(engine)
    return engine,sessionmaker(bind=engine,autoflush=False)


def projection(db):
    return {'gates':[(r.feature_key,r.required_tier,r.description) for r in db.scalars(select(FeatureGate).order_by(FeatureGate.feature_key))],
        'limits':[(r.tier,r.feature_key,r.limit_value) for r in db.scalars(select(PlanLimit).order_by(PlanLimit.tier,PlanLimit.feature_key))]}


def test_complete_defaults_use_two_reads_without_changes_or_commits(monkeypatch):
    engine,sessions=factory()
    with sessions() as db:
        e.seed_feature_gates(db);e.seed_plan_limits(db)
        before=projection(db)
    with sessions() as db:
        calls=[]
        def count(conn,cursor,statement,parameters,context,executemany):calls.append(statement.split()[0])
        event.listen(engine,'before_cursor_execute',count)
        def forbidden():raise AssertionError('Complete defaults do not need a commit')
        monkeypatch.setattr(db,'commit',forbidden)
        e.seed_feature_gates(db);e.seed_plan_limits(db)
        event.remove(engine,'before_cursor_execute',count)
        assert calls==['SELECT','SELECT']
        assert projection(db)==before
    engine.dispose()


def test_partial_defaults_preserve_admin_overrides_and_unrelated_rows():
    engine,sessions=factory()
    gate=next(iter(e.DEFAULT_FEATURE_GATES))
    tier=next(iter(e.ENTITLEMENTS));feature=next(iter(e.ENTITLEMENTS[tier].limits))
    with sessions() as db:
        db.add_all([FeatureGate(feature_key=gate,required_tier='pro',description='Admin override'),
            FeatureGate(feature_key='legacy_feature',required_tier='premium',description='Preserved'),
            PlanLimit(tier=tier,feature_key=feature,limit_value=123456),
            PlanLimit(tier='legacy',feature_key='legacy_feature',limit_value=77)])
        db.commit()
        e.seed_feature_gates(db);e.seed_plan_limits(db)
        value=projection(db)
        assert len(value['gates'])==len(e.DEFAULT_FEATURE_GATES)+1
        assert len(value['limits'])==sum(len(t.limits) for t in e.ENTITLEMENTS.values())+1
        assert (gate,'pro','Admin override') in value['gates']
        assert (tier,feature,123456) in value['limits']
        assert ('legacy','legacy_feature',77) in value['limits']
        for name,config in e.DEFAULT_FEATURE_GATES.items():
            if name!=gate:assert (name,config['required_tier'],config['description']) in value['gates']
        e.seed_feature_gates(db);e.seed_plan_limits(db)
        assert projection(db)==value
    engine.dispose()


def test_check_does_not_hide_a_later_deleted_default_in_same_session():
    engine,sessions=factory()
    with sessions() as db:
        e.seed_feature_gates(db);e.seed_plan_limits(db)
        gate=next(iter(e.DEFAULT_FEATURE_GATES))
        db.delete(db.get(FeatureGate,gate));db.commit()
        e.seed_feature_gates(db)
        assert db.get(FeatureGate,gate).required_tier==e.DEFAULT_FEATURE_GATES[gate]['required_tier']
    engine.dispose()
