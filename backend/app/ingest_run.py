import os
from app.ingest_house import ingest_house

if __name__ == "__main__":
    pages = int(os.getenv("INGEST_PAGES", "3"))
    limit = int(os.getenv("INGEST_LIMIT", "200"))
    print(ingest_house(pages=pages, limit=limit))
