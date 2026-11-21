# Shopping Sessions: C++ high-performance pipeline.
# Multi-stage build for linux/amd64 (Intel Xeon Cascade Lake+ scorers).
# gcc 15 builder: ~5% less CPU than bookworm's gcc 12 on this code (measured). Same Debian
# release (bookworm, glibc 2.36) as the runtime image; libstdc++/libgcc are linked statically
# because the runtime image only ships gcc 12's libstdc++.
FROM gcc:15.2.0-bookworm AS builder

WORKDIR /build
COPY pipeline/ /build/pipeline/
# -mtune=cascadelake: scorer is GCP N2 Cascade Lake (probe-measured); instruction set stays x86-64-v3
RUN g++ -std=c++20 -O3 -march=x86-64-v3 -mtune=cascadelake -pthread \
    -static-libstdc++ -static-libgcc \
    /build/pipeline/pipeline.cpp -o /build/pipeline_bin

FROM debian:bookworm-slim

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY --from=builder /build/pipeline_bin /app/pipeline_bin
COPY run.sh /app/run.sh
RUN chmod +x /app/pipeline_bin /app/run.sh

WORKDIR /app
CMD ["/app/pipeline_bin"]
