FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH=/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl poppler-utils tesseract-ocr tzdata \
    && rm -rf /var/lib/apt/lists/*

ARG SUPERCRONIC_VERSION=v0.2.34
ARG TARGETARCH=amd64
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) SUPERCRONIC_ARCH=amd64; SUPERCRONIC_SHA1SUM=e8631edc1775000d119b70fd40339a7238eece14 ;; \
      arm64) SUPERCRONIC_ARCH=arm64; SUPERCRONIC_SHA1SUM=4ab6343b52bf9da592e8b4bb7ae6eb5a8e21b71e ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH"; exit 1 ;; \
    esac; \
    SUPERCRONIC=supercronic-linux-$SUPERCRONIC_ARCH; \
    curl -fsSLO "https://github.com/aptible/supercronic/releases/download/$SUPERCRONIC_VERSION/$SUPERCRONIC"; \
    echo "$SUPERCRONIC_SHA1SUM  $SUPERCRONIC" | sha1sum -c -; \
    chmod +x "$SUPERCRONIC"; \
    mv "$SUPERCRONIC" "/usr/local/bin/$SUPERCRONIC"; \
    ln -s "/usr/local/bin/$SUPERCRONIC" /usr/local/bin/supercronic; \
    supercronic -version

# Install dependencies
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Verify backfill module exists in deployed package
RUN python -c "import app.backfill_events_from_trades"

# Ensure SQLite directory exists and declare it as a volume
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
