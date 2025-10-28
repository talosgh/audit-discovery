# syntax=docker/dockerfile:1

FROM node:20-bullseye-slim AS ui-build
WORKDIR /dashboard
COPY dashboard/package.json ./
RUN npm install
COPY dashboard/ ./
RUN npm run build

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
COPY --from=build /app/sql ./sql
COPY --from=build /app/README.md ./
COPY --from=ui-build /dashboard/dist ./static
ENV WEBHOOK_PORT=8080 \
    API_PREFIX=/webhook \
    STATIC_DIR=/srv/audit-webhook/static
EXPOSE 8080
ENTRYPOINT ["./audit_webhook"]
