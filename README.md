# kafka-publisher

Small **local dev** web app to publish JSON messages to Kafka using [KafkaJS](https://kafka.js.org/). It serves a static UI and a `POST /api/publish` API from one Node process.

**Security:** There is **no authentication** on the UI or API. Only run this on trusted networks (e.g. your laptop). Do not expose it to the internet.

## Layout

| Path | Role |
|------|------|
| `package.json` | Node 20+, ESM, dependencies |
| `Dockerfile` | Production-style image: `node:20-alpine`, `npm ci`, runs `src/server.js` |
| `docker-compose.yml` | Publishes port `3010`, attaches to external Docker network `bff` (see below) |
| `.dockerignore` | Excludes `node_modules`, `.env`, etc. from the image build context |
| `src/server.js` | Express: `/api/publish`, static files from `public/` |
| `src/producer.js` | Singleton Kafka producer, shared across requests |
| `public/index.html`, `app.js`, `styles.css` | Browser UI |

## Environment variables

| Variable | Purpose |
|----------|---------|
| `KAFKA_BROKERS` | **Required.** Comma-separated brokers (e.g. `kafka:9092` in Docker on `bff`, `localhost:29092` from the host, or integration bootstrap servers). |
| `PORT` | HTTP port (default `3010`). |
| `KAFKA_CLIENT_ID` | Kafka client id (default `kafka-test-publisher`). |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` | When both are set, SASL is enabled (integration-style clusters). |
| `KAFKA_SASL_MECHANISM` | Default `scram-sha-512`. |
| `KAFKA_SECURITY_PROTOCOL` | Default `SASL_PLAINTEXT` when username/password are set; use `SASL_SSL` or `SSL` for TLS to the broker. |

Optional: copy `.env.example` to `.env` (loaded automatically via `dotenv`).

## Run on the host (next to Docker Kafka)

From the machine where Docker exposes Kafka (e.g. `localhost:29092` for the default broker):

```bash
cd kafka-publisher
npm install
KAFKA_BROKERS=localhost:29092 npm start
```

Open http://localhost:3010 (or your `PORT`).

### Web UI

- Enter the **topic** only (full name, e.g. `int.offer.offer_domain`).
- **Messages:** paste a JSON array, or use **Choose JSON file…** to load a Kafdrop-style export (must be one JSON array: `[ { … }, … ]`). Per-message `headers`, `key`, and `value` live inside each object; there are no separate header/key fields in the form.
- If you submit with an empty text area but a file is still selected, that file is read again.

This does **not** require joining the `bff` network; it only needs a reachable broker address.

## Run with Docker (same network as your service)

Your service's `docker-compose.yml` can define an attachable network named **`bff`**. This project's `docker-compose.yml` joins that **external** network and defaults to **`KAFKA_BROKERS=kafka:9092`** so the container talks to the same hostname as services inside that compose file.

### Prerequisites

1. **Create `bff` and run Kafka** — this app does **not** start ZooKeeper or Kafka. From your service directory, bring up at least:

   ```bash
   cd ../your-service   # or your path to your service
   docker compose up -d zookeeper kafka
   ```

   Confirm the broker is there: `docker compose ps` should list `kafka` (and `zookeeper`) as running.

2. **Then start kafka-publisher** — from this directory:

   ```bash
   docker compose up --build
   ```

3. Open http://localhost:3010.

If your compose profile uses another broker service (e.g. `kafka1`), override when starting:

```bash
KAFKA_BROKERS=kafka1:9093 docker compose up --build
```

### Why this matches sports-service consumers

- On **`bff`**, the bootstrap host **`kafka:9092`** is the same service sports-service uses in `dockerlocal` / `dockerlocalsbx`-style configs.
- You must still publish to the **exact topic name** (including prefix such as `int.…`) that the consumer subscribes to.

### Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| `getaddrinfo ENOTFOUND kafka` (here or in sports-service) | The **`kafka`** container is not running, or the app is not on `bff`. Start `zookeeper` + `kafka` in sports-service compose; for sports-service in the `build` container, use `docker compose exec build getent hosts kafka` — it should resolve. |
| Sports-service logs `otel-collector:4317` DNS errors | **Unrelated to Kafka.** The `build` service points OpenTelemetry at `otel-collector`; start that stack or run with `OTEL_SDK_DISABLED=true` when exec’ing sports-service (see sports-service docs / compose). |
| Publish succeeds but consumers see nothing | Wrong **topic** or wrong **cluster** (e.g. regional broker). Align `KAFKA_BROKERS` and topic with the consumer’s config. |

## API

**`POST /api/publish`** — JSON body:

- `topic` (string, required): Full topic name as consumers use it (no automatic prefix).
- `messages` (array, required): At least one element. Each element is interpreted automatically:
  - **Plain DTO** (default): the object is `JSON.stringify`’d and sent as the record value. Optional top-level `headers` / `key` apply to every record (see below).
  - **Envelope / Kafdrop-style export** (detected when an element has `value.payload` plus `value.encoding` and/or `headers` / `key`): metadata fields such as `partitionID`, `offset`, and `timestamp` are ignored. The publisher maps:
    - **Headers:** `headers[]` with `{ "key": "…", "value": { "payload": "…", "encoding": "text"|"json"|"none", "schemaId": … } }` → Kafka headers (UTF-8; `json` payload objects are stringified for the wire).
    - **Key:** `key`: `{ "payload": "<string>", "encoding": "text" }` or `encoding: "none"` / empty payload → omit key unless the JSON body includes optional top-level `key` (string) as a fallback for all records.
    - **Value:** `value`: `{ "payload": <object|string>, "encoding": "json"|… }` → record bytes (`json` uses `JSON.stringify` on `payload`).
  - **Double-wrapped export:** If `value.payload` is itself a full message object (inner `headers`, `key`, `value`), that inner layer is unwrapped once so the real event and headers are used (fixes accidental “message inside value” JSON).
- `headers` (object, optional): For API/scripts only — string keys and string values merged into **every** record; envelope header entries override on duplicate keys. The web UI does not send this; put headers in each envelope in `messages`.
- `key` (string, optional): For API/scripts only — default Kafka key when an envelope has no usable key (`encoding: none`, etc.).

**Response:** `{ "ok": true, "published": <number> }` or `{ "ok": false, "error": "..." }` with 4xx/5xx.

## Producer lifecycle

A single producer is created on first publish and disconnected on **SIGINT** / **SIGTERM** after the HTTP server closes.

## Examples

**Local broker (host → Docker Kafka published as 29092):**

```bash
KAFKA_BROKERS=localhost:29092 npm start
```

**Integration-style SASL** (align with your `sports-service` / `template.env` credentials):

```bash
export KAFKA_BROKERS=kb001.example:9092
export KAFKA_USERNAME=local.mock
export KAFKA_PASSWORD='…'
export KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
npm start
```
