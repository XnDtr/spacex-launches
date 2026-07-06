FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sql/ sql/
COPY scripts/ scripts/
COPY analysis/ analysis/

# /data is meant to be a mounted volume -- keeps the generated db/charts
# outside the image, consistent with the repo itself not committing them.
VOLUME ["/data"]

CMD ["sh", "-c", "python scripts/ingest.py --db /data/spacex.db --log-file /data/ingest.log && python analysis/analysis.py --db /data/spacex.db --out /data/output"]
