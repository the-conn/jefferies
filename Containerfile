FROM quay.io/the-conn/jefferies-test as builder

WORKDIR /usr/src/jefferies
COPY . .

RUN make build

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y \
  ca-certificates \
  libssl-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/jefferies/target/release/jefferies /app/jefferies
COPY --from=builder /usr/src/jefferies/config /app/config

USER 1001

EXPOSE 8080
CMD ["./jefferies"]
