# News Ingestion Pipeline

> 🚧 **Work in progress** — pipeline and semantic search are stable; web app is under active development.

An end-to-end asynchronous news ingestion and semantic search system. Fetches articles from RSS feeds and public APIs, deduplicates them, stores metadata in SQLite, generates vector embeddings, and exposes a web interface for keyword and semantic search.

---

## Architecture

```
Producer (round-robin across topic buckets)
        ↓
URL Queue (asyncio.Queue)
        ↓
Fetchers × 3 (concurrent content extraction)
        ↓
Content Queue (asyncio.Queue)
        ↓
Persistor (JSONL + SQLite, URL-deduplicated)
        ↓
Embedder (sentence-transformers → pgvector)
        ↓
Web App (FastAPI + semantic/keyword search UI)
```

- **Producer** – Discovers article URLs via RSS and NewsAPI across configurable topic buckets. Seeds `seen_urls` from SQLite on startup to avoid redundant fetches.
- **Fetchers** – Concurrently fetch and extract article content using `httpx` + `trafilatura`. Hash content for deduplication.
- **Persistor** – Upserts article metadata into SQLite (`ON CONFLICT(url) DO UPDATE`). Streams JSONL for traceability.
- **Embedder** – Encodes article content with `all-MiniLM-L6-v2` and upserts 384-dim vectors into pgvector.
- **Web App** – FastAPI backend serving a single-page UI with paginated browsing, keyword search, and semantic search.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Async runtime | Python 3.11 `asyncio` |
| HTTP client | `httpx` |
| Content extraction | `trafilatura` |
| Metadata store | SQLite (`sqlite3`) |
| Vector store | PostgreSQL + `pgvector` |
| Embeddings | `sentence-transformers` (`all-MiniLM-L6-v2`) |
| Web framework | FastAPI + Uvicorn |
| Containerisation | Docker Compose |

---

## Quickstart

### With Docker

```bash
cp src/.env.example src/.env
# fill in NEWSAPI_KEY, NEWSAPI_HOST, NEWSAPI_URL

docker-compose up
```

### Locally

```bash
pip install -r requirements.txt

# Run the ingestion pipeline
python -m src.pipeline.run

# Generate embeddings
python -m src.pipeline.embedder

# Start the web app
python web_app.py
```

Open `http://localhost:8000` in your browser.

---

## Configuration

Environment variables (copy from `src/.env.example`):

| Variable | Description |
|---|---|
| `NEWSAPI_URL` | RapidAPI endpoint |
| `NEWSAPI_KEY` | RapidAPI key |
| `NEWSAPI_HOST` | RapidAPI host header |
| `LOG_DIR` | Log output directory (default: `src/logs`) |
| `TARGET_TOTAL` | Max URLs to discover per run |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/articles` | Paginated article list (`limit`, `offset`, `bucket`, `signal_type`) |
| GET | `/api/articles/search` | Keyword search (`q`, `limit`, `bucket`) |
| GET | `/api/articles/semantic-search` | Vector similarity search (`q`, `limit`, `min_similarity`) |
| GET | `/api/filters` | Distinct buckets and signal types in the corpus |
| GET | `/api/stats` | Dataset overview (counts, publishers, date range) |
| GET | `/api/semantic-search/stats` | Embedding coverage stats |

---

## Project Structure

```
news_topic_model/
├── src/
│   ├── api_adapter/        # RSS and NewsAPI clients
│   ├── pipeline/           # Producer, fetcher, persistor, embedder, orchestrator
│   ├── services/           # SemanticSearchService
│   ├── storage/            # SQLite and pgvector store helpers
│   └── utils/              # URL normalisation, logging, feed config
├── web/
│   ├── index.html
│   └── static/             # app.js, style.css
├── tests/                  # Unit and integration tests
├── web_app.py              # FastAPI application entry point
├── docker-compose.yml
└── requirements.txt
```

---

## Author

**Ayomide Abass**
Data Engineer | Async Systems | Data Analytics
Vancouver, Canada
[LinkedIn](https://www.linkedin.com/in/ayomide-abass-36b40025a/) · [GitHub](https://github.com/ayoabass777)

---

MIT License © 2025 Ayomide Abass
