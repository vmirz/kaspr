FROM twmvmirz/py3.12-rocksdb8.1.1:latest-r1

# Workaround for RocksDB memory leak
# (https://blog.cloudflare.com/the-effect-of-switching-to-tcmalloc-on-rocksdb-memory-use/)
ENV MALLOC_ARENA_MAX=2

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kaspr/ ./kaspr/

CMD ["python", "-m", "kaspr", "worker", "-l", "info"]