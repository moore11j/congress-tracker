FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH=/app

# Install dependencies
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Ensure SQLite directory exists and declare it as a volume
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8080

ENTRYPOINT ["uvicorn", "app.main:app", "--host", "::", "--port", "8080"]
