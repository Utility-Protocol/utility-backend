FROM node:20-bookworm-slim

# better-sqlite3 needs a toolchain to build its native binding.
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 make g++ ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package*.json ./
RUN npm ci

COPY . .

RUN mkdir -p /app/data
ENV NODE_ENV=production
ENV PORT=4000
ENV DB_PATH=/app/data/utility_indexer.db
EXPOSE 4000

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -fsS http://localhost:4000/health || exit 1

CMD ["node", "src/index.js"]
