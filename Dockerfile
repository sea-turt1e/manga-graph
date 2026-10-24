FROM python:3.15.1-slim

WORKDIR /app

# eMudhra CA issue (https://emudhra.we-have-a-problem.com/):
# upgrade the system CA store and pip to the patched versions
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir "pip>=26.2.2"

COPY requirements.txt .
# torch / sentence-transformers are not installed: no Python 3.15 wheels yet,
# and the API server does not use them (only the embedding scripts do)
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
