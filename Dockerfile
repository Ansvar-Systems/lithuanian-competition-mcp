FROM node:20-slim AS builder
WORKDIR /app

# Install build deps for native modules (better-sqlite3)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 make g++ \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
# Run full install (including postinstall) so better-sqlite3 native binding is built
RUN npm ci

COPY tsconfig.json ./
COPY src/ src/
RUN npm run build

FROM node:20-slim AS production
WORKDIR /app
ENV NODE_ENV=production
ENV KT_LT_DB_PATH=/app/data/kt-lt.db

# Copy node_modules from builder (preserves better-sqlite3 native binding)
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist/ dist/
COPY package.json ./

# Database (workflow's Provision step writes data/database.db before build)
COPY data/database.db data/kt-lt.db

RUN addgroup --system --gid 1001 mcp \
 && adduser --system --uid 1001 --ingroup mcp mcp \
 && chown -R mcp:mcp /app
USER mcp

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

CMD ["node", "dist/src/http-server.js"]
