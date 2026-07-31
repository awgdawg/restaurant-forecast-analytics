# Cloud Run Job image for rfa-sync (extraction moved off-workspace; the
# serverless egress wall blocks Toast -- see the 2026-07-30 design spec).
# Installs the full package for one-source-of-truth deps; image weight from
# prophet/cmdstanpy is accepted (job cold-start is seconds either way).
# NOTE: ZoneInfo("America/Chicago") in ingest/sync.py needs tzdata, which
# arrives transitively via pandas>=2.2 -- if pandas is ever dropped from the
# image, add an explicit tzdata dependency or sync.py dies at import.
FROM python:3.11-slim
# Unbuffered stdout: Cloud Logging must see progress lines in real time and
# must not lose buffered output on SIGKILL (task timeout / OOM).
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY ingest/ ingest/
COPY load/ load/
COPY forecast/ forecast/
COPY publish/ publish/
RUN pip install --no-cache-dir .
ENTRYPOINT ["rfa-sync"]
CMD ["--refresh-days", "3"]
