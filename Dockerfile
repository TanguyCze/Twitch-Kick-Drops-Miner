FROM python:3.11-slim

LABEL maintainer="Twitch-Kick Drops Miner"
LABEL description="Unified Headless Twitch & Kick Drops Miner with Web UI"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MINER_HOST=0.0.0.0 \
    MINER_PORT=8920 \
    MINER_DATA_DIR=/app/data

WORKDIR /app

# Install system dependencies required by curl_cffi / aiohttp on Linux
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY core/ ./core/
COPY web/ ./web/
COPY main.py .

RUN mkdir -p /app/data

VOLUME ["/app/data"]
EXPOSE 8920

CMD ["python", "main.py"]
