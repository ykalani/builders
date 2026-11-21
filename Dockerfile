FROM rust:1.100-bookworm AS build
WORKDIR /src
COPY rewrites/rust/ ./
RUN cargo build --release --locked
FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping-sessions-rust /app/shopping-sessions
CMD ["/app/shopping-sessions"]
