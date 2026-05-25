.PHONY: dev dev-down run-api run-engine run-frontend build-frontend build up down logs clean check

# ── Local Development ────────────────────────────────────────────
# Start MongoDB + Redis containers for local dev
dev:
	docker compose -f deploy/docker-compose.dev.yml up -d

dev-down:
	docker compose -f deploy/docker-compose.dev.yml down

# Run Rust services natively (requires `make dev` first)
run-api:
	MONGO_URL=mongodb://127.0.0.1:27017 REDIS_URL=redis://127.0.0.1:6379 \
	cargo run --bin apiserver

run-engine:
	MONGO_URL=mongodb://127.0.0.1:27017 REDIS_URL=redis://127.0.0.1:6379 \
	cargo run --bin engine

# Run frontend dev server (Vite, auto-proxy /api to apiserver:3000)
run-frontend:
	cd frontend && npm run dev

# Install frontend dependencies
install-frontend:
	cd frontend && npm install

# Build frontend for production
build-frontend:
	cd frontend && npm run build

# ── Full Deployment ──────────────────────────────────────────────
build:
	docker compose -f deploy/docker-compose.yml build

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

logs:
	docker compose -f deploy/docker-compose.yml logs -f

# ── Utilities ────────────────────────────────────────────────────
check:
	cargo check

clean:
	cargo clean
	docker compose -f deploy/docker-compose.dev.yml down -v
	docker compose -f deploy/docker-compose.yml down -v

# ── Code Quality Gates ───────────────────────────────────────────
# Generate coverage report (required before "crap" check)
coverage:
	cargo llvm-cov --workspace --lcov --output-path lcov.info

# Run CRAP analysis with gate (fail if any function exceeds threshold)
crap:
	cargo crap --workspace --lcov lcov.info --fail-above

# Generate coverage + run CRAP gate in one step
qc: coverage crap

# Save a baseline (run on main branch only)
crap-baseline:
	cargo crap --workspace --lcov lcov.info --format json --output baseline.json
