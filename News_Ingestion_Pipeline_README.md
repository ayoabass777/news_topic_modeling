# 📰 News Ingestion Pipeline

## 🚀 Overview

An end-to-end asynchronous pipeline that ingests and deduplicates news articles from public APIs, processes their content, and prepares them for analysis.  
The system is designed for modularity, fault tolerance, and scalability — serving as a foundation for building intelligent content discovery and topic modeling systems.

---

## 🧱 Architecture

```text
[ Load Balancer ]
       ↓
[ Producer Worker ] 
       ↓
[ URL Queue ]
       ↓
[ Fetcher Worker ]
       ↓
[ Content Queue ]
       ↓
[ Persistor Worker ]
       ↓
[ SQLite + JSONL + Parquet ]
```

- **Producer** – Uses a load balancer to distribute requests across categories (e.g., politics, sports).  
- **Fetcher** – Downloads and hashes article content; normalizes text with [Trafilatura](https://github.com/adbar/trafilatura).  
- **Persistor** – Writes JSONL streams, buffers Parquet files, and maintains an SQLite database for metadata and deduplication.  
- **Orchestrator** – Manages all tasks using `asyncio.TaskGroup`, monitors queue sizes, and ensures clean shutdowns through sentinels.  

---

## ⚙️ Tech Stack

- **Language:** Python 3.11  
- **Core Libraries:** `asyncio`, `aiohttp`, `trafilatura`, `pyarrow`, `sqlite3`  
- **Data Formats:** JSONL, Parquet, SQLite  
- **Architecture:** Event-driven async pipeline with backpressure monitoring  
- **Orchestration:** Custom supervisor and monitor tasks built using structured concurrency  

---

## 🗃️ Data Persistence & Idempotency

Each article is persisted across three formats:

| Format   | Description | Purpose |
|-----------|--------------|----------|
| `.jsonl` | Streamed write | Line-by-line traceable ingestion |
| `.parquet` | Buffered batch | Efficient columnar format for analytics |
| `.db (SQLite)` | Upsert table | Deduplication and metadata tracking |

SQLite ensures **idempotent writes** using a composite unique key on `url` and `content_id`.

---

## 🧩 Pipeline Coordination

### Orchestrator (`run.py`)
Handles the lifecycle of all tasks in parallel:
- Supervised execution (`supervise()`) with failure propagation  
- Queue monitoring every 5 seconds  
- Graceful cancellation and sentinel handoff between stages  
- Logs backpressure to detect bottlenecks  

### Supervised Flow
```bash
Producer → URL Queue → Fetcher → Content Queue → Persistor
```

If any stage crashes, all others are safely cancelled and logs provide trace diagnostics.

---

## 🧠 Future Enhancements

- Integration with **Kafka or Redis Streams** for real-time streaming  
- **Embedding-based topic clustering** using `sentence-transformers`  
- REST endpoint for querying and monitoring ingestion metrics  

---

## 💻 Running Locally

```bash
# Clone the repository
git clone https://github.com/ayoabass777/news-pipeline.git
cd news-pipeline

# Create and activate a virtual environment
python -m venv env
source env/bin/activate  # (On Windows: env\Scripts\activate)

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python src/pipeline/run.py
```

---

## 📂 Output Directory

```bash
/data/
│
├── articles.jsonl      # Raw streamed output
├── articles.parquet    # Cleaned structured data
└── articles.db         # Metadata store (SQLite)
```

---

## 📸 Preview

![News Pipeline Dashboard](assets/news_pipeline_screenshot.png)

---

## 🧾 Author

**Ayomide Abass**  
Data Engineer | Async Systems | Data Analytics  
📍 Vancouver, Canada  
🔗 [LinkedIn](https://www.linkedin.com/in/ayomide-abass)  
🔗 [GitHub](https://github.com/ayoabass777)

---

## 📜 License

MIT License © 2025 Ayomide Abass
