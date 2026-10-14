# Qubetics Native Indexer Service

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Node](https://img.shields.io/badge/node-18.x-green)
![TypeScript](https://img.shields.io/badge/TypeScript-5.x-blue)

A TypeScript/Node.js service that indexes the **Qubetics** Cosmos-SDK (Tendermint) chain into PostgreSQL. It streams blocks and transactions live, backfills missed and downtime ranges, keeps validator, delegation, proposal and token-price data current, and pushes real-time updates over Socket.IO.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Database](#database)
- [Running](#running)
- [Docker](#docker)
- [Project Structure](#project-structure)
- [Scripts](#scripts)
- [Health Check](#health-check)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

---

## Architecture

```
            ┌──────────────────────────┐
            │  Qubetics Tendermint Node│
            │   (WebSocket + RPC/REST) │
            └────────────┬─────────────┘
                         │ new blocks / txs
                         ▼
┌────────────┐   ┌───────────────────┐   ┌──────────────┐
│  RabbitMQ  │◄──┤  Native Indexer   ├──►│    Redis     │
│  (queues)  │──►│  (Node.js / TS)   │   │   (cache)    │
└────────────┘   └───┬───────────┬───┘   └──────────────┘
                     │           │
                     ▼           ▼
             ┌────────────┐  ┌──────────────┐
             │ PostgreSQL │  │  Socket.IO   │
             │  (Prisma)  │  │  clients     │
             └────────────┘  └──────────────┘
```

**On startup the service:**

1. Loads the environment and connects to PostgreSQL, Redis and RabbitMQ.
2. Subscribes to new blocks and transactions over the Tendermint WebSocket.
3. Backfills blocks and transactions missed while the service was down.
4. Starts the scheduled jobs:
   - validators, proposals and derived metrics, every **5 s**
   - token price from CoinMarketCap, every **1 h**
5. Emits live events to connected Socket.IO clients.

**RabbitMQ queues**

| Queue                                        | Purpose                           |
| -------------------------------------------- | --------------------------------- |
| `NATIVE_LATEST_BLOCKS` / `NATIVE_LATEST_TXS` | Live blocks and transactions      |
| `NATIVE_MISSED_BLOCK` / `NATIVE_MISSED_TXS`  | Re-processing of missed data      |
| `NATIVE_DOWNTIME_BLOCK` / `NATIVE_DOWNTIME_TXS` | Backfill after downtime        |
| `NATIVE_ERROR_BLOCK`                         | Blocks that failed processing     |
| `LATEST_PROPOSALS`                           | Governance proposal updates       |

## Tech Stack

| Layer     | Technology                                 |
| --------- | ------------------------------------------ |
| Runtime   | Node.js 18, TypeScript 5                   |
| HTTP / WS | Express, Socket.IO, `ws`                   |
| Chain     | CosmJS, Tendermint RPC                     |
| Database  | PostgreSQL 16, Prisma ORM 5                |
| Cache     | Redis 7                                    |
| Messaging | RabbitMQ 3 (`amqplib`)                     |
| Tooling   | ESLint, Prettier, Husky, Jest, Nodemon     |

## Prerequisites

- **Node.js** 18.x and npm
- **PostgreSQL** 16+
- **Redis** 7+
- **RabbitMQ** 3+
- Access to a **Qubetics node**: Tendermint WebSocket, Tendermint RPC and Cosmos REST (Swagger) endpoints
- *(Optional)* **Docker** and Docker Compose for the local infrastructure

## Getting Started

```bash
# 1. Clone
git clone https://github.com/Qubetics/native-indexer-service.git
cd native-indexer-service

# 2. Install dependencies
npm install

# 3. Start local infrastructure (Postgres, Redis, RabbitMQ, pgAdmin)
docker compose up -d

# 4. Configure environment
cp env.example .env        # then edit the values

# 5. Generate the Prisma client and apply migrations
npm run generate:prisma
npx prisma migrate deploy

# 6. Run in development mode
npm run dev
```

## Configuration

The env file is chosen by `NODE_ENV`:

| `NODE_ENV`               | File loaded  |
| ------------------------ | ------------ |
| `production`             | `.env.prod`  |
| `staging`                | `.env.stag`  |
| `test`                   | `.env.test`  |
| `local` / anything else  | `.env`       |

**Environment variables**

| Variable                        | Required | Default    | Description                                           |
| ------------------------------- | :------: | ---------- | ----------------------------------------------------- |
| `PORT`                          |          | `3146`     | HTTP server port                                      |
| `NODE_ENV`                      |          | `dev`      | Selects the runtime environment and env file          |
| `DATABASE_URL`                  |    ✅    |            | PostgreSQL connection string                          |
| `NATIVE_TENDERMINT_NODE_WS_URL` |    ✅    |            | Tendermint WebSocket endpoint                         |
| `NATIVE_TENDERMINT_HTTP_HOST`   |    ✅    |            | Tendermint RPC endpoint                               |
| `NATIVE_SWAGGER_HTTP_HOST`      |    ✅    |            | Cosmos REST (LCD) endpoint                            |
| `EVM_API_BASE_URL`              |          |            | EVM API base URL                                      |
| `REDIS_HOST`                    |    ✅    |            | Redis host                                            |
| `REDIS_PORT`                    |    ✅    |            | Redis port                                            |
| `RABITMQ_URL`                   |    ✅    |            | RabbitMQ AMQP URL                                     |
| `ADDRESS_PREFIX`                |    ✅    |            | Bech32 address prefix of the chain                    |
| `TOKEN_SYMBOL`                  |          | `TICS`     | Native token symbol                                   |
| `TOKEN_CURRENCY`                |          |            | Fiat currency for price conversion (for example `USD`) |
| `INITIAL_SUPPLY`                |          |            | Initial token supply, used for supply metrics         |
| `COIN_MARKET_API_PATH`          |          |            | CoinMarketCap quotes endpoint                         |
| `COIN_MARKET_API_KEY`           |          |            | CoinMarketCap API key                                 |
| `PROJECT_NAME`                  |          | `qubetics` | Project identifier, also the HTTP route prefix        |
| `TTL_TIME_FOR_REWARDS`          |          | `1`        | Cache TTL for rewards data                            |
| `SECRET_KEY`                    |          |            | Application secret                                    |
| `SALT_VALUE`                    |          | `10`       | Hashing salt rounds                                   |

> ⚠️ Never commit real `.env*` files or credentials.

## Database

The schema is in [`prisma/schema.prisma`](prisma/schema.prisma). The core models are `Blocks`, `Transactions`, `Validator`, `Delegator`, `ValidatorStake`, `Proposals`, `Voters`, `Rewards`, `Tokens`, `Holders`, `Contracts`, `ContractLogs` and `CoinmarketInfo`.

```bash
npm run generate:prisma     # Regenerate the Prisma client
npm run migrate:dev         # Create and apply a migration (development)
npx prisma migrate deploy   # Apply pending migrations (staging/production)
npm run db:studio           # Open Prisma Studio
```

## Running

| Mode        | Command                                                |
| ----------- | ------------------------------------------------------ |
| Development | `npm run dev`                                          |
| Local       | `npm run start:local`                                  |
| Production  | `npm run build:prod && npm run start:production`       |
| Staging     | `npm run build:stag && npm run start:staging`          |

## Docker

```bash
# Build the image
docker build -t qubetics-native-indexer .

# Run it
docker run -d --name native-indexer \
  -p 7500:7500 \
  -v $(pwd)/.env.prod:/usr/src/app/.env.prod \
  qubetics-native-indexer
```

The image builds the TypeScript sources, generates the Prisma client and runs `npm run start:production`. It exposes port `7500`, so set `PORT=7500` in `.env.prod`.

`docker-compose.yml` starts only the local dependencies:

| Service  | Port(s)                                 |
| -------- | --------------------------------------- |
| Postgres | `5432`                                  |
| Redis    | `6380` (host) → `6379` (container)      |
| RabbitMQ | `5672`, management UI at `15672`        |
| pgAdmin  | `5050`                                  |

CI/CD workflows are in [`.github/workflows`](.github/workflows), and the Kubernetes manifest is [`deployment.yaml`](deployment.yaml).

## Project Structure

```
.
├── prisma/                 # Prisma schema and migrations
├── src/
│   ├── server.ts           # Entry point: bootstraps services and jobs
│   ├── App.ts              # Express app and middleware
│   ├── routes.ts           # HTTP routes
│   ├── cron/               # Scheduled jobs (validators, proposals, price)
│   ├── helper/             # Block, tx, validator, proposal and WS logic
│   ├── service/            # RabbitMQ, Redis and Socket.IO services
│   ├── lib/                # Database client and logger
│   ├── environments/       # Environment loading
│   ├── constant/           # Queue names, Redis keys, constants
│   ├── interface/          # Shared TypeScript types
│   └── Utility/            # Common utilities
├── Dockerfile
├── docker-compose.yml
├── deployment.yaml
└── env.example
```

## Scripts

| Script                      | Description                     |
| --------------------------- | ------------------------------- |
| `npm run dev`               | Start with Nodemon (hot reload) |
| `npm run build:prod`        | Compile TypeScript to `build/`  |
| `npm run lint` / `lint-fix` | Run ESLint / fix lint errors    |
| `npm run pretty`            | Format with Prettier            |
| `npm test`                  | Run the Jest tests with coverage |

## Health Check

```http
GET /<PROJECT_NAME>/status        # default: /qubetics/status
→ 200 { "message": "success" }
```

## Contributing

1. Fork the repository and create a feature branch from `master`.
2. Follow the existing code style. Husky runs lint and Prettier on commit.
3. Write clear commit messages and open a pull request with a description of the change.

## Security

To report a vulnerability, follow [SECURITY.md](SECURITY.md). Please do not open public issues for security problems.

## License

This project is licensed under the [MIT License](LICENSE).

Copyright © 2026 Qubetics
