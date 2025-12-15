FROM rust:1.91-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    make \
    build-essential \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src

#COPY zerofs/Cargo.toml zerofs/Cargo.lock ./zerofs/
#COPY zerofs/src ./zerofs/src
COPY zerofs ./zerofs/

WORKDIR /usr/src/zerofs


#RUN RUSTFLAGS="--cfg tokio_unstable" OUT_DIR="/usr/src/zerofs/src/rpc" cargo build --release
RUN RUSTFLAGS="--cfg tokio_unstable" cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
   ca-certificates \
   libssl3 \
   && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/zerofs/target/release/zerofs /usr/local/bin/zerofs

RUN useradd -m -u 1001 zerofs
USER zerofs

# Default ports that might be used - actual configuration comes from TOML file
EXPOSE 2049 5564 10809

CMD ["zerofs"]
