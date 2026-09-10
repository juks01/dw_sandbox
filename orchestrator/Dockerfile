FROM python:3.14-slim

RUN pip install --no-cache-dir psycopg[binary]

COPY run.py /run.py

CMD ["python", "/run.py"]
