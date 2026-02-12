# ---- Build stage ----
FROM rust:1.93-bookworm AS builder

# Build arguments for profile selection:
#   CARGO_PROFILE  - "release" (default) or "profiling"
#   CARGO_FEATURES - "" (default) or "--features profiling"
ARG CARGO_PROFILE=release
ARG CARGO_FEATURES=""

WORKDIR /workspace

# Cache dependency build: copy only manifests, build a dummy main, then remove.
COPY interceptor-rs/Cargo.toml interceptor-rs/Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() { println!("dummy"); }' > src/main.rs \
    && cargo build --profile ${CARGO_PROFILE} ${CARGO_FEATURES} 2>/dev/null; \
    rm -rf src

# Build the real binary (only src/ changes trigger a rebuild).
COPY interceptor-rs/src/ src/
RUN touch src/main.rs && cargo build --profile ${CARGO_PROFILE} ${CARGO_FEATURES}

# ---- Runtime stage ----
# Note: the binary path depends on the profile name.
# "release" -> target/release/    "profiling" -> target/profiling/
FROM gcr.io/distroless/cc-debian12:nonroot
ARG CARGO_PROFILE=release
COPY --from=builder /workspace/target/${CARGO_PROFILE}/interceptor-rs /sbin/init
ENTRYPOINT ["/sbin/init"]
