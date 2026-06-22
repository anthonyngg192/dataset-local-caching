# ---- build stage ----
FROM rust:1-slim-bookworm AS builder
WORKDIR /app

# Cache dependencies: copy manifests first, build deps against a stub main.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --bin dataset-local && \
    rm -rf src

# Now the real sources.
COPY src ./src
RUN touch src/main.rs && cargo build --release --bin dataset-local

# ---- runtime stage ----
FROM debian:bookworm-slim
RUN useradd --system --uid 10001 dataset
COPY --from=builder /app/target/release/dataset-local /usr/local/bin/dataset-local

USER dataset
EXPOSE 8383
ENV RUST_LOG=info
# Auth is off unless BOTH are set:
#   docker run -e DATASET_USERNAME=admin -e DATASET_PASSWORD=secret ...
# Worker count defaults to CPU count; override with WORKERS.

ENTRYPOINT ["dataset-local"]
