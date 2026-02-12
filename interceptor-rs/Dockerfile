# ---- Build stage ----
FROM rust:1.93-bookworm AS builder

WORKDIR /workspace

# Cache dependency build: copy only manifests, build a dummy main, then remove.
COPY interceptor-rs/Cargo.toml interceptor-rs/Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() { println!("dummy"); }' > src/main.rs \
    && cargo build --release 2>/dev/null; \
    rm -rf src

# Build the real binary (only src/ changes trigger a rebuild).
COPY interceptor-rs/src/ src/
RUN touch src/main.rs && cargo build --release

# ---- Runtime stage ----
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /workspace/target/release/interceptor-rs /sbin/init
ENTRYPOINT ["/sbin/init"]
