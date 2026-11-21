# Compile the native stages at build time; no compiler is needed at run time.
FROM python:3.12-slim-bookworm AS native
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir -r /build/requirements.txt
COPY pipeline/ /build/pipeline/
COPY scripts/build_native.py /build/scripts/
RUN g++ -std=c++17 -O3 -pthread -Wall -Wextra -Werror /build/pipeline/aggregate.cpp -o /aggregate
RUN python /build/scripts/build_native.py g++ /aggregate.so

# Shopping Sessions: native in-memory processing and Arrow Parquet output.
# The scorers are linux/amd64 (Intel Xeon). They build this file natively, so
# no FROM line pins a platform: `make build` builds for linux/amd64 anywhere.
FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline/ /app/pipeline/
COPY --from=native /aggregate /app/pipeline/aggregate
COPY --from=native /aggregate.so /app/pipeline/aggregate.so
COPY run.sh /app/run.sh
RUN sed -i 's/\r$//' /app/run.sh && chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
CMD ["/app/run.sh"]
