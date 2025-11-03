# 📰 News Ingestion Pipeline

## 🚀 Overview

An end-to-end asynchronous pipeline that ingests and deduplicates news articles from public APIs, processes their content, and prepares them for analysis.  
The system is designed for modularity, fault tolerance, and scalability — a foundation for building intelligent content discovery, topic modelling, or downstream analytics.

---

## 🧱 Architecture

```text
Producer (round robin per bucket)
        ↓
URL Queue (asyncio.Queue)
        ↓
Fetchers × 3 (content extraction)
        ↓
Content Queue (asyncio.Queue)
        ↓
Persistor (JSONL + Parquet + SQLite)
```

- **Producer** – Discovers URLs via NewsAPI/RapidAPI queries per topic bucket and enqueues enriched discovery records.
- **Fetcher Workers** – Concurrently fetch article content with `httpx`, normalise with `trafilatura`, and produce hash IDs.
- **Persistor** – Streams JSONL, buffers Parquet batches with `pyarrow`, and upserts metadata into SQLite for deduplication.
- **Run Orchestrator** – Uses `asyncio.TaskGroup` for supervised execution, queue monitoring, and graceful shutdown (sentinel fan-out).

---

## ⚙️ Requirements

Python 3.11+

```bash
pip install -r requirements.txt
```

Key packages: `httpx`, `trafilatura`, `aiolimiter`, `pyarrow`, `pydantic`, `python-dotenv`.

---

## 🔐 Configuration

Copy the example environment file and fill in your RapidAPI credentials:

```bash
cp src/.env.example src/.env
# edit src/.env with NEWSAPI_URL/KEY/HOST values
```

Environment variables loaded via `python-dotenv`:

- `NEWSAPI_URL` – RapidAPI endpoint for NewsAPI
- `NEWSAPI_KEY` – RapidAPI key
- `NEWSAPI_HOST` – RapidAPI host header
- Optional overrides: `LOG_DIR`, `TARGET_TOTAL`

---

## ▶️ Running the Pipeline Locally

```bash
python -m src.pipeline.run
```

Logs are written to `src/logs/pipeline.*.log` and console; data outputs stream into `src/data/` (JSONL, Parquet, SQLite).

---

## 🧪 Testing

The pipeline relies on integration runs. Before running locally, ensure dependencies are installed and credentials are valid. Add unit tests around API adapters, producers, and fetchers as the project evolves.

---

## 📁 Repository Layout

```
src/
├── api_adapter/      # External API clients (NewsAPI)
├── pipeline/         # Async producer/fetcher/persistor orchestration
├── storage/          # SQLite storage helpers
├── utils/            # Shared utilities (logging, helpers)
├── data/             # Runtime outputs (ignored in VCS)
├── logs/             # Log files (ignored in VCS)
├── .env.example      # Sample environment configuration
└── README.md         # This document
```

---

## 📄 License

MIT License © 2025 Ayomide Abass
