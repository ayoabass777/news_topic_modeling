# 📰 News Topic Modeling Web App - MVP

A modern web interface for exploring and querying news articles using GraphRAG (Graph-based Retrieval Augmented Generation).

## 🚀 Features

- **Browse Articles**: Paginated view of all ingested news articles
- **Search**: Keyword-based search across article titles and content
- **GraphRAG Query**: Natural language queries with entity extraction and relationship mapping
- **Statistics**: Dataset overview with article counts, publishers, and date ranges

## 🛠️ Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure you have news data in the `data/silver_parquet/` directory (from the ingestion pipeline)

## ▶️ Running the Application

Start the FastAPI server:

```bash
python web_app.py
```

Or using uvicorn directly:

```bash
uvicorn web_app:app --reload --host 0.0.0.0 --port 8000
```

Then open your browser and navigate to:
```
http://localhost:8000
```

## 📁 Project Structure

```
news_topic_model/
├── web_app.py                    # FastAPI application
├── web/
│   ├── index.html               # Frontend HTML
│   └── static/
│       ├── style.css            # Styling
│       └── app.js               # Frontend JavaScript
├── src/news_topic_model/services/
│   └── graphrag_service.py      # GraphRAG service (MVP implementation)
└── data/
    └── silver_parquet/          # News article data
```

## 🔌 API Endpoints

### GET `/api/articles`
Get paginated list of articles
- Query params: `limit` (default: 10), `offset` (default: 0)

### GET `/api/articles/search`
Search articles by keyword
- Query params: `q` (search term), `limit` (default: 10)

### POST `/api/graphrag/query`
Query news content using GraphRAG
- Body: `{"query": "your question", "max_results": 10}`

### GET `/api/stats`
Get dataset statistics
- Returns: total articles, unique publishers, date range

## 🧠 GraphRAG Implementation

The current implementation is an **MVP** that provides:

1. **Basic Semantic Search**: Keyword matching across articles
2. **Entity Extraction**: Simple keyword-based entity identification
3. **Answer Generation**: Summary responses based on matched articles
4. **Relationship Mapping**: Placeholder structure for future graph relationships

### Future Enhancements

To extend this to full GraphRAG capabilities, consider:

1. **Knowledge Graph Construction**:
   - Use NER models (spaCy, transformers) for entity extraction
   - Extract relationships between entities
   - Build a knowledge graph from news articles

2. **Graph-based Retrieval**:
   - Implement graph traversal algorithms
   - Use graph embeddings for semantic search
   - Integrate with Microsoft GraphRAG or similar frameworks

3. **LLM Integration**:
   - Use LLMs (GPT, Claude, etc.) for answer generation
   - Context-aware responses based on graph structure
   - Multi-hop reasoning across entities

4. **Advanced Features**:
   - Topic clustering and visualization
   - Temporal analysis of news trends
   - Entity relationship graphs
   - Citation and source tracking

## 🎨 Frontend

The frontend is a single-page application with:
- Modern, responsive design
- Tab-based navigation
- Real-time API interactions
- Clean article display cards

## 📝 Notes

- The GraphRAG service currently uses simple keyword matching as an MVP
- For production use, integrate with a full GraphRAG implementation
- The frontend assumes the API is running on the same origin (CORS may need configuration for cross-origin requests)

## 🔧 Configuration

The application automatically:
- Loads articles from `data/silver_parquet/` directory
- Serves static files from `web/static/`
- Initializes GraphRAG service on startup

## 📄 License

MIT License © 2025 Ayomide Abass
