FROM python:3.14-slim

RUN pip install --no-cache-dir psycopg[binary]

COPY load.py /app/load.py

CMD ["python", "/app/load.py"]
