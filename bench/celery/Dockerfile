FROM python:3.12-slim

RUN pip install --no-cache-dir "celery[redis]==5.4.*"

WORKDIR /app
COPY tasks.py enqueue.py ./
