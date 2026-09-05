# Smart Context AI

Document ingest and retrieval API. Upload returns immediately; a Kafka worker parses, chunks, and embeds; Postgres stores text and vectors; `POST /v1/ask` streams citations and an answer.

Not a chat UI. OpenAI is used for embeddings (`text-embedding-3-small`) and chat (`gpt-4o-mini`) only.

## Architecture

```
POST /v1/documents
        → disk + documents row (ACCEPTED)
        → Kafka  document.uploaded
Kafka consumer
        → claim row (skip duplicates)
        → parse / chunk / embed (Redis cache)
        → chunks (pgvector + tsvector) → READY
POST /v1/ask
        → optional X-API-Key + Redis rate limit
        → hybrid search (vector + keyword RRF)
        → SSE: status, citation, token, done
```

If embeddings or chat fail (including HTTP 429), chunks and keyword/latest retrieval still run; ask returns citations and passage text.

## Stack

| Layer | Choice |
| --- | --- |
| API | Java 20, Spring Boot 3.4, WebFlux |
| DB | Postgres 16 + pgvector, R2DBC; Flyway over JDBC at startup |
| Queue | Kafka (`document.uploaded`) |
| Cache / limits | Redis |
| Parse | PDFBox + text files |
| Metrics | Actuator, Prometheus scrape |

## Run

Docker Desktop, then:

```bash
docker compose up -d
```

Postgres is mapped to **5433** (host 5432 is often already taken).

```powershell
$env:OPENAI_API_KEY = "sk-..."
gradle bootRun --no-daemon
```

```bash
export OPENAI_API_KEY=sk-...
gradle bootRun --no-daemon
```

Optional: `$env:APP_API_KEYS = "dev-key"` then send `X-API-Key` on `/v1` routes except health.


```
GET  http://localhost:8080/v1/health
GET  http://localhost:8080/actuator/prometheus
gradle test --no-daemon
```

Health should report `app`, `postgres`, and `redis`.

## API

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/v1/health` | App, Postgres, Redis |
| POST | `/v1/documents` | multipart field `file` |
| GET | `/v1/documents/{id}` | Poll until `READY` or `FAILED` |
| POST | `/v1/documents/{id}/reindex` | Re-run ingest on the stored file |
| DELETE | `/v1/documents/{id}` | 204; chunks cascade |
| POST | `/v1/ask` | SSE |

`{id}` is a UUID only (36 characters, no braces).

Upload:

```http
POST /v1/documents
Content-Type: multipart/form-data
```

Ask:

```json
{
  "query": "What is this document about?",
  "documentIds": ["00000000-0000-0000-0000-000000000000"]
}
```

`documentIds` is optional. Omit or `[]` to search all documents.

SSE events: `status`, `citation` (filename, chunk index, ids, score), `token`, `done`. Errors use event `error`.

Rate limit: 60 requests/minute per API key (or `anonymous`). Redis down → requests are allowed.

## Screenshots

### Docker Compose

<img width="1152" height="192" alt="image" src="https://github.com/user-attachments/assets/ae0eb5e7-72c9-4bb4-8123-9b394b33e09a" />


### Health

<img width="537" height="188" alt="image" src="https://github.com/user-attachments/assets/8758f882-2341-4479-9172-bf3f51696f3f" />


### Upload (ACCEPTED)

<img width="807" height="620" alt="image" src="https://github.com/user-attachments/assets/6ea6c1db-fdbe-4a68-9924-59785d16f8cd" />


### Document status (READY)

<img width="610" height="246" alt="image" src="https://github.com/user-attachments/assets/2dca970b-6abc-4902-ba9e-9977c3dc2c3d" />

### Citations

<img width="630" height="287" alt="image" src="https://github.com/user-attachments/assets/3f14b958-104d-4986-9d78-3fb7dff08a55" />


### Metrics 
<img width="944" height="384" alt="image" src="https://github.com/user-attachments/assets/6fbdc164-68e3-44df-aef7-d5bb8c5144db" />



## Layout

```
src/main/java/com/smartcontext/
  controller/     HTTP
  service/        ingest, ask, OpenAI, cache, rate limit
  repository/     R2DBC
  filter/         API key, rate limit
  config/
src/main/resources/db/migration/
```

## Config

| Variable | Purpose |
| --- | --- |
| `OPENAI_API_KEY` | Embeddings and chat |
| `APP_API_KEYS` | Comma-separated keys; empty = open API |

Needs OpenAI **API** credit. ChatGPT Plus does not fund this.

## Limits

- Flyway is JDBC at process start only; request path is R2DBC.
- Kafka consumer claims a row so a retried message does not re-ingest `READY` documents unless reindex is called.
- No UI. Call the HTTP API (Postman, curl).
