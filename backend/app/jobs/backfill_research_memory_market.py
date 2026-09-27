"""Freeze historical references for pre-existing Research Memories, once."""
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.db import SessionLocal, ensure_research_memory_schema
from app.models import ResearchThesis, ResearchThesisMarketBaseline
from app.services.research_memory_market import capture_baseline


def main():
    ensure_research_memory_schema()
    count = 0
    with SessionLocal() as db:
        ids = db.execute(select(ResearchThesis.id).outerjoin(
            ResearchThesisMarketBaseline, ResearchThesisMarketBaseline.thesis_id == ResearchThesis.id,
        ).where(ResearchThesisMarketBaseline.thesis_id.is_(None))).scalars().all()
        for thesis_id in ids:
            try:
                capture_baseline(db, db.get(ResearchThesis, thesis_id), historical=True)
                db.commit()
                count += 1
            except IntegrityError:
                db.rollback()  # Another run already captured this immutable baseline.
    print(f"Historical thesis baselines captured: {count}")


if __name__ == "__main__":
    main()
