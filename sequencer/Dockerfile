# syntax=docker/dockerfile:1

FROM debian:bookworm-slim AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace/sequencer
COPY sequencer/ ./

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --target g12_sequencer test_sequencer -j2 \
    && ./build/test_sequencer

FROM debian:bookworm-slim AS runtime
WORKDIR /app

# Match the Java Gateway UID/GID. /dev/shm itself is shared via Compose IPC.
USER 10001:10001

COPY --from=build --chown=10001:10001 \
    /workspace/sequencer/build/g12_sequencer /usr/local/bin/g12_sequencer

ENTRYPOINT ["/usr/local/bin/g12_sequencer"]
CMD ["--no-pin"]
