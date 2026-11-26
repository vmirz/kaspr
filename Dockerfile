ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim-trixie

# Workaround for RocksDB memory leak
# (https://blog.cloudflare.com/the-effect-of-switching-to-tcmalloc-on-rocksdb-memory-use/)
ENV MALLOC_ARENA_MAX=2

WORKDIR /app
COPY requirements.txt .
# RocksDB comes with rocksdict, built into its wheels: the image needs none of its own.
RUN pip install --no-cache-dir -r requirements.txt

COPY kaspr/ ./kaspr/
# A pod would compile it as it starts, within its CPU limit.
RUN python -m compileall -q kaspr

CMD ["python", "-m", "kaspr", "worker", "-l", "info"]
