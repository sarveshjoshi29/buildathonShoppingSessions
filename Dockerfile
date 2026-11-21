# Shopping Sessions: C++ high-performance pipeline.
# Multi-stage build for linux/amd64 (Intel Xeon Cascade Lake+ scorers).
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY pipeline/ /build/pipeline/
# probe branch: -DSCORER_PROBE encodes scorer facts in the run time (see pipeline.cpp)
RUN g++ -std=c++20 -O3 -march=x86-64-v3 -pthread -DSCORER_PROBE /build/pipeline/pipeline.cpp -o /build/pipeline_bin

FROM debian:bookworm-slim

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY --from=builder /build/pipeline_bin /app/pipeline_bin
COPY run.sh /app/run.sh
RUN chmod +x /app/pipeline_bin /app/run.sh

WORKDIR /app
CMD ["/app/pipeline_bin"]
