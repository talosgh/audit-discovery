# syntax=docker/dockerfile:1

FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    unzip \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Makefile README.md env.example ./
COPY src/ src/
COPY sql/ sql/
RUN make && strip audit_webhook

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    unzip \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /srv/audit-webhook
COPY --from=build /app/audit_webhook ./
COPY sql ./sql
COPY README.md .
USER root
ENV WEBHOOK_PORT=8080
EXPOSE 8080
ENTRYPOINT ["./audit_webhook"]
