## Architecture Overview

Water Bottle is a RAG (Retrieval-Augmented Generation) system with multiple integrations:

### Core Components

**API Layer**
- FastAPI web server with health checks
- RESTful endpoints for chat and Telegram webhooks
- Environment-based configuration management

**RAG Pipeline**
- **Embedder**: Sentence-transformers for vector embeddings
- **Retriever**: Elasticsearch for document search
- **Generator**: Google Gemini via LiteLLM for responses
- **Memory**: Session-based conversation history

**Integrations**
- **Telegram Bot**: Webhook-based Q&A bot
- **ETL Pipeline**: Data ingestion from multiple sources
- **Vector Databases**: Pinecone and Elasticsearch support

### Architecture Flow

```
User Query → Telegram/FastAPI → Input Parser → RAG Pipeline → Response Generator → User
                                    ↓
                              Vector Search
                                    ↓
                              Context Retrieval
                                    ↓
                              LLM Generation
```

### Directory Structure

```
water-bottle/
├── api/                    # FastAPI application
│   ├── application.py     # Main app setup
│   ├── routers/           # API endpoints
│   └── utils/             # Helper utilities
├── services/              # Business logic
│   ├── rag/              # RAG workflow
│   └── telegram/         # Telegram integration
├── src/etl/              # Data pipeline
│   ├── connectors/       # Database connections
│   ├── loaders/          # Data ingestion
│   └── credentials/      # Credential management
├── config.yaml           # Configuration file
├── .env                  # Environment variables
├── Dockerfile           # Container setup
└── requirements.txt      # Dependencies
```

### Key Features

- **Multi-channel**: Web API and Telegram bot
- **Cloud-ready**: Docker containerization with environment variables
- **Scalable**: Factory pattern for connectors and loaders
- **Secure**: Environment-based credential management
- **Extensible**: Plugin architecture for new integrations

### Quick Start

1. Set up environment variables (see `installation_guide.txt`)
2. Run `uvicorn api.application:app --reload`
3. Send messages to your Telegram bot or use the web API

For detailed setup instructions, see [installation_guide.txt](installation_guide.txt).