# Wikipedia Pageviews ETL Pipeline

A production-ready Apache Airflow data pipeline that analyzes Wikipedia pageview statistics for major tech companies (Amazon, Apple, Facebook, Google, Microsoft) to support sentiment analysis for stock market prediction.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Airflow 3.1+](https://img.shields.io/badge/airflow-3.1+-blue.svg)](https://airflow.apache.org/)
[![UV](https://img.shields.io/badge/package%20manager-uv-purple)](https://github.com/astral-sh/uv)

## Project Overview

This capstone project demonstrates data workflow orchestration with Apache Airflow by implementing a complete ETL pipeline that:

- **Extracts** hourly Wikipedia pageview data from Wikimedia dumps
- **Transforms** raw data to extract company-specific metrics
- **Loads** processed data into PostgreSQL database
- **Analyzes** pageview trends to identify companies with highest interest

### Business Context

The project supports **CoreSentiment**, a stock market prediction tool that leverages Wikipedia pageview data as a sentiment indicator. The hypothesis: increased pageviews correlate with positive sentiment and potential stock price increases.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Airflow Scheduler                        │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │   DAG Executor    │
                    └─────────┬─────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   ┌────▼────┐         ┌──────▼──────┐      ┌──────▼──────┐
   │Download │         │   Extract   │      │    Load     │
   │  Task   │────────▶│    Task     │─────▶│    Task     │
   └─────────┘         └─────────────┘      └──────┬──────┘
        │                                           │
        │                                           ▼
   ┌────▼────────────────────────────────────┐   ┌─────────────┐
   │  Wikimedia Pageview Dumps              │   │  PostgreSQL │
   │  https://dumps.wikimedia.org           │   │   Database  │
   └────────────────────────────────────────┘   └─────────────┘
```

### Pipeline Flow

1. **Initialize Database** → Create schema and tables
2. **Download** → Fetch random hourly pageview dump (~50MB gzipped)
3. **Extract** → Parse gzip, filter company data, aggregate views
4. **Load** → Insert data into PostgreSQL with idempotence
5. **Analyze** → Query database for company with highest views
6. **Cleanup** → Remove temporary files (optional)

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+** (required)
- **PostgreSQL 12+** (database)
- **UV** (will be auto-installed if missing)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/wikipedia-pageview-pipeline.git
cd wikipedia-pageview-pipeline

# Run the setup script (installs UV if needed)
chmod +x scripts/*.sh
./scripts/setup.sh

# Configure your database credentials
cp .env.example .env
nano .env  # Edit with your PostgreSQL credentials
```

### Database Setup

```bash
# Create PostgreSQL database
psql -U postgres
CREATE DATABASE company_pageviews;
CREATE USER airflow_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE company_pageviews TO airflow_user;
\q
```

Update `.env` file:
```env
DB_HOST=localhost
DB_NAME=company_pageviews
DB_USER=airflow_user
DB_PASSWORD=your_password
```

### Running the Pipeline

```bash
# Start Airflow
./scripts/run_local.sh

# Access Airflow UI
# Open browser: http://localhost:8080
# Username: admin
# Password: admin
```

## Project Structure

```
wikipedia-pageview-pipeline/
├── dags/
│   ├── config.py                    # Centralized configuration
│   ├── wikipedia_pageview_dag.py    # Main DAG definition
│   └── utils/
│       ├── __init__.py
│       ├── download_data.py         # Download utilities
│       ├── extract_data.py          # Data extraction logic
│       ├── load_to_db.py            # Database operations
│       └── logger.py                # Logging configuration
├── scripts/
│   ├── setup.sh                     # UV-based setup script
│   └── run_local.sh                 # Run Airflow locally
├── sql/
│   └── analysis_queries.sql         # SQL analysis queries
├── tests/
│   └── test_pipeline.py             # Unit tests (optional)
├── data/                            # Downloaded files (gitignored)
├── logs/                            # Application logs
├── airflow/                         # Airflow home directory
├── .env.example                     # Environment template
├── .env                             # Your configuration (gitignored)
├── .gitignore
├── pyproject.toml                   # UV/Python dependencies
└── README.md                        # This file
```

## ⚙️ Configuration

### Environment Variables (.env)

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `company_pageviews` |
| `DB_USER` | Database user | `airflow_user` |
| `DB_PASSWORD` | Database password | *(required)* |
| `WIKI_BASE_URL` | Wikipedia dumps URL | `https://dumps.wikimedia.org/...` |
| `CHUNK_SIZE` | Rows per chunk | `500000` |
| `MAX_RETRIES` | Task retry limit | `3` |
| `RETRY_DELAY_SECONDS` | Retry delay | `300` |
| `ENABLE_CLEANUP` | Auto-cleanup files | `true` |
| `ENABLE_EMAIL_ALERTS` | Send failure emails | `false` |

## Key Features & Best Practices

### ✅ Implemented Best Practices

1. **Idempotence**
   - Database uses `ON CONFLICT` to handle duplicate inserts
   - Unique constraint on `(company_name, source_file)`
   - Safe to re-run without data duplication

2. **Error Handling**
   - Custom exceptions for each module
   - Try-catch blocks in all critical sections
   - Graceful degradation and cleanup on failure

3. **Retry Logic**
   - Configurable retry attempts (default: 3)
   - Exponential backoff (300 seconds)
   - Task-level timeout (2 hours)

4. **Monitoring & Alerts**
   - Comprehensive logging with emojis
   - Email alerts on failure (configurable)
   - SLA monitoring capability
   - Task execution timeout enforcement

5. **Data Validation**
   - Gzip file integrity checks
   - CSV schema validation
   - Data type verification
   - Negative value detection

6. **Resource Management**
   - Automatic cleanup of temporary files
   - Chunked file processing (memory efficient)
   - Database connection pooling
   - Configurable retention policies

7. **Security**
   - Environment variable configuration
   - No hardcoded credentials
   - `.env` file excluded from git
   - Database password encryption support

8. **Observability**
   - Detailed task documentation
   - XCom for inter-task communication
   - Structured logging with context
   - Performance metrics tracking

## Database Schema

### Table: `company_pageviews`

```sql
CREATE TABLE company_pageviews (
    id SERIAL PRIMARY KEY,
    company_name TEXT NOT NULL,
    pageviews INTEGER NOT NULL CHECK (pageviews >= 0),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_company_file UNIQUE (company_name, source_file)
);

CREATE INDEX idx_company_pageviews_created ON company_pageviews(created_at DESC);
CREATE INDEX idx_company_pageviews_company ON company_pageviews(company_name);
```

### View: `company_pageviews_summary`

```sql
CREATE VIEW company_pageviews_summary AS
SELECT 
    company_name,
    SUM(pageviews) as total_pageviews,
    COUNT(*) as load_count,
    MAX(created_at) as last_updated
FROM company_pageviews
GROUP BY company_name
ORDER BY total_pageviews DESC;
```

## 🔍 Analysis Queries

### Find Company with Highest Pageviews

```sql
-- Main requirement query
SELECT 
    company_name,
    SUM(pageviews) as total_pageviews
FROM company_pageviews
GROUP BY company_name
ORDER BY total_pageviews DESC
LIMIT 1;
```

### Top 5 Companies with Percentage

```sql
SELECT 
    company_name,
    SUM(pageviews) as total_pageviews,
    ROUND(SUM(pageviews) * 100.0 / SUM(SUM(pageviews)) OVER (), 2) as percentage
FROM company_pageviews
GROUP BY company_name
ORDER BY total_pageviews DESC
LIMIT 5;
```

More queries available in `sql/analysis_queries.sql`

## Testing

### Run Unit Tests

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest tests/ -v --cov=dags

# Run specific test
pytest tests/test_pipeline.py::test_download -v
```

### Manual Testing

```bash
# Test individual DAG tasks
uv run airflow tasks test wikipedia_pageviews_pipeline download_pageviews 2025-10-23

# List all DAGs
uv run airflow dags list

# Validate DAG structure
uv run airflow dags test wikipedia_pageviews_pipeline
```

## Monitoring & Debugging

### View Logs

```bash
# Application logs
tail -f logs/pipeline_*.log

# Airflow scheduler logs
tail -f logs/scheduler.log

# Airflow task logs
# Available in Airflow UI: http://localhost:8080
```

### Common Issues & Solutions

**Issue: Database connection failed**
```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Test connection
psql -h localhost -U airflow_user -d company_pageviews
```

**Issue: DAG not appearing in UI**
```bash
# Check DAG syntax
uv run python dags/wikipedia_pageview_dag.py

# Refresh DAGs
uv run airflow dags reserialize
```

**Issue: Import errors**
```bash
# Ensure PYTHONPATH is set
export PYTHONPATH="$(pwd):$PYTHONPATH"

# Reinstall dependencies
uv pip install -e .
```

## Deployment Options

### Option 1: Local Development (Current Setup)
- Uses SQLite for Airflow metadata
- LocalExecutor for task execution
- Suitable for development and testing

### Option 2: Production with Docker
```bash
# Coming soon - Docker Compose setup
docker-compose up -d
```


## Design Decisions & Rationale

### Why UV Instead of pip?

1. **Speed**: 10-100x faster than pip for package installation
2. **Reliability**: Better dependency resolution
3. **Modern**: Built with Rust, actively maintained
4. **Simplicity**: Single tool for venv, install, and run

### Why LocalExecutor?

- Simple setup for single-machine deployment
- Parallel task execution
- Better than SequentialExecutor
- No need for Celery/Redis overhead

### Why PostgreSQL?

- ACID compliance for data integrity
- Better SQL support than SQLite
- Production-ready for scaling
- Native Airflow metadata store support

### Why Chunked Processing?

- Memory efficiency (handles 200MB+ files)
- Allows progress tracking
- Enables early termination if needed
- Better error recovery

### Why Random File Selection?

- Simplifies initial implementation
- Demonstrates pipeline flexibility
- Can be enhanced to:
  - Select specific date/time
  - Process multiple files
  - Incremental backfill


### Code Style

- Follow PEP 8 guidelines
- Use type hints where applicable
- Add docstrings to functions
- Run `black` and `ruff` before committing

```bash
# Format code
black dags/

# Lint code
ruff check dags/
```

## 👥 Authors

- **Idris Akorede Ibrahim** 

## References

- [Wikipedia Pageviews Documentation](https://dumps.wikimedia.org/other/pageviews/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [UV Package Manager](https://github.com/astral-sh/uv)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## Contact

For questions or support, please open an issue on GitHub or contact [idrisakoredeibrahim@gmail.com]

---

**Built with ❤️ for Data Engineering**