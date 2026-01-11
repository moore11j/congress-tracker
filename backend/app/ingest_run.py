from app.ingest_house import ingest_house

if __name__ == "__main__":
    # smaller limits to start; we can increase after we confirm stability
    print(ingest_house(pages=2, limit=100))
