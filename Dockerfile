FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_pipeline/ etl_pipeline/
WORKDIR /app/etl_pipeline

CMD ["python", "pipeline.py"]