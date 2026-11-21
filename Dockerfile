# Compile the native stages at build time; no compiler is needed at run time.
FROM python:3.12-slim-bookworm AS native
RUN apt-get update && apt-get install -y --no-install-recommends g++ rustc nlohmann-json3-dev && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir -r /build/requirements.txt
COPY pipeline/ /build/pipeline/
COPY scripts/build_native.py /build/scripts/
RUN mkdir /release && python /build/scripts/build_native.py g++ /release/shopping-sessions --executable --rust-csv
RUN cp -L /usr/local/lib/python3.12/site-packages/pyarrow/libarrow.so.* \
          /usr/local/lib/python3.12/site-packages/pyarrow/libparquet.so.* /release/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=native /release/ /app/
WORKDIR /app
CMD ["/app/shopping-sessions"]
