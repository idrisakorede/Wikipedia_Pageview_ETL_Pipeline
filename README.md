# 📊 Core Sentiment Analytics Pipeline

A minimalistic, production-ready data pipeline that analyzes Wikipedia pageview data to track public interest in major tech companies (Amazon, Apple, Google, Microsoft, Meta) using LLM-powered filtering and real-time dashboards.

![Pipeline Status](https://img.shields.io/badge/status-production-brightgreen)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Airflow](https://img.shields.io/badge/airflow-3.1.0-orange)

---

## 🎯 Project Overview

This pipeline:
1. **Downloads** hourly Wikipedia pageview data from Wikimedia dumps
2. **Filters** noise using SQL-based rules (removes lists, meta pages, people, etc.)
3. **Classifies** pages by company using Python keyword matching
4. **Analyzes** with local LLM (Ollama) to identify genuine product/service pages
5. **Visualizes** results in an interactive Streamlit dashboard
6. **Notifies** via Slack/Email on success/failure

**Key Features:**
- ✅ Fully containerized with Docker Compose
- ✅ Local LLM processing (no API costs)
- ✅ Minimalistic design (3 tables, 4 views)
- ✅ Real-time analytics dashboard
- ✅ Email & Slack notifications
- ✅ Parallel task execution

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    AIRFLOW DAG                          │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  1. Database Setup (DDL)                                │
│  2. Extract & Load Raw (Wikipedia → PostgreSQL)         │
│  3. Transform (Prefilter → LLM Filter)                  │
│  4. Load Filtered & Analytics                           │
│  5. Cleanup & Notifications                             │
│                                                         │
└─────────────────────────────────────────────────────────┘
           │                    │                    │
           ▼                    ▼                    ▼
   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
   │  PostgreSQL  │    │    Ollama    │    │  Streamlit   │
   │  (Database)  │    │    (LLM)     │    │ (Dashboard)  │
   └──────────────┘    └──────────────┘    └──────────────┘
```

---

## 📦 Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 3.1.0 | Workflow management |
| **Database** | PostgreSQL 16 | Data storage |
| **LLM** | Ollama (Llama 3.2:1b) | Local AI filtering |
| **Dashboard** | Streamlit + Plotly | Data visualization |
| **Containerization** | Docker + Docker Compose | Deployment |
| **Notifications** | Slack + Gmail | Alerts |

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- 8GB+ RAM (for Ollama LLM)
- Internet connection (for Wikipedia downloads)

### 1. Clone & Configure

```bash
git clone <your-repo-url>
cd CoreSentiment_Data_Pipeline

# Create environment file
cp .env.example .env
nano .env  # Add your credentials
```

### 2. Configure `.env`

```bash
# Database
DB_NAME=core_sentiment_db
DB_USER=dki
DB_PASSWORD=your_secure_password

# Ollama LLM
OLLAMA_HOST=http://ollama:11434
OLLAMA_MODEL=llama3.2:1b
OLLAMA_TIMEOUT=300

# Email Notifications
ENABLE_EMAIL_ALERTS=true
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
EMAIL_FROM=your-email@gmail.com
EMAIL_TO=recipient@gmail.com

# Slack Notifications
ENABLE_SLACK=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### 3. Start Services

```bash
# Start all containers
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### 4. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | admin / airflow |
| **Streamlit Dashboard** | http://localhost:8501 | - |
| **PostgreSQL** | localhost:5434 | See `.env` |
| **Ollama** | http://localhost:11434 | - |

### 5. Run Pipeline

**Option A: Airflow UI**
1. Go to http://localhost:8080
2. Unpause DAG: `Core_Sentiment_Data_Pipeline`
3. Click "Trigger DAG"

**Option B: CLI**
```bash
docker exec -it airflow-worker airflow dags trigger Core_Sentiment_Data_Pipeline
```

---

## 📁 Project Structure

```
CoreSentiment_Data_Pipeline/
├── dags/
│   ├── pageviews.py                          # Main DAG definition
│   └── core_sentiment/
│       ├── dashboard.py                       # Streamlit dashboard
│       └── include/
│           ├── app/
│           │   ├── tasks/                     # Task implementations
│           │   │   ├── download_data.py
│           │   │   ├── extract_data.py
│           │   │   ├── prefilter_data.py
│           │   │   ├── llm_filter.py
│           │   │   ├── load_raw_data.py
│           │   │   ├── load_filtered_data.py
│           │   │   ├── notifications.py
│           │   │   └── cleanup.py
│           │   └── utils/
│           │       └── pageviews_filtering_prompt.py
│           ├── app_config/
│           │   └── settings.py                # Configuration
│           └── sql/
│               ├── ddl/
│               │   └── tables.sql             # Database schema
│               └── queries/
│                   ├── company_rankings.sql
│                   └── biggest_company.sql
├── docker-compose.yaml                        # Container orchestration
├── Dockerfile                                 # Airflow image
├── Dockerfile.streamlit                       # Dashboard image
├── requirements.txt                           # Python dependencies
└── .env                                       # Environment variables
```

---

## 🗄️ Database Schema

### Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `raw_pageviews` | Immutable warehouse | domain, page_title, count_views, processing_date |
| `filtered_pageviews` | LLM-filtered data | page_title, company, count_views, filter_method |
| `sentiment_results` | Future sentiment analysis | page_title, company, sentiment_label, sentiment_score |

### Views

| View | Purpose |
|------|---------|
| `v_daily_company_summary` | Daily aggregated metrics by company |
| `v_data_quality_metrics` | Pipeline health KPIs |
| `v_top_pages_by_company` | Top 10 pages per company (last 7 days) |
| `v_sentiment_distribution` | Sentiment breakdown (future) |

---

## 🔄 Pipeline Stages

### 1. Database Setup
Creates tables, indexes, and views in PostgreSQL.

### 2. Extract & Load Raw
- Downloads Wikipedia pageview dump (gzipped)
- Extracts all pageviews
- Loads to `raw_pageviews` table (~10M rows/day)

### 3. Transform
**Prefilter (SQL):**
- Filters by minimum views (1000+)
- Removes noise (lists, meta pages, people, etc.)
- Applies tech company keywords
- Classifies by company (Python)

**LLM Filter (Ollama):**
- Batches remaining rows (50 at a time)
- Uses Llama 3.2:1b to identify genuine products/services
- Removes false positives

### 4. Load Filtered & Analytics
- Loads filtered data to `filtered_pageviews`
- Runs analytics queries
- Calculates company rankings

### 5. Cleanup & Notifications
- Removes temporary files
- Sends success/failure notifications

---

## 📊 Dashboard Features

The Streamlit dashboard provides:

- **🏆 Winner Analysis**: Market leader vs runner-up comparison
- **📈 Key Metrics**: Total pages, views, averages
- **🏅 Company Rankings**: Detailed metrics table + pie chart
- **📅 Daily Trends**: Time series chart showing view evolution
- **🔝 Top Pages**: Tabs for each company's top 15 pages
- **🔍 Data Quality**: Raw vs filtered records tracking
- **🔎 Data Explorer**: Search and download capabilities

---

## 🔔 Notifications

### Email Setup (Gmail)

1. Enable 2FA on your Google account
2. Generate App Password: https://myaccount.google.com/apppasswords
3. Add to `.env`:
```bash
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=xxxx xxxx xxxx xxxx  # App password
EMAIL_TO=recipient@gmail.com
```

### Slack Setup

1. Create Slack app: https://api.slack.com/apps
2. Enable Incoming Webhooks
3. Add webhook to channel
4. Add to `.env`:
```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

---

## 🛠️ Maintenance

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-worker
docker-compose logs -f streamlit
docker-compose logs -f ollama
```

### Restart Services

```bash
# Restart all
docker-compose restart

# Restart specific service
docker-compose restart airflow-worker
docker-compose restart streamlit
```

### Database Access

```bash
# Connect to database
docker exec -it core_sentiment-db psql -U dki -d core_sentiment_db

# Run queries
SELECT COUNT(*) FROM raw_pageviews;
SELECT COUNT(*) FROM filtered_pageviews;
SELECT * FROM v_daily_company_summary LIMIT 5;
```

### Clear Cache

```bash
# Clear Streamlit cache
docker exec -it core_sentiment_streamlit rm -rf /root/.streamlit/cache

# Rebuild images
docker-compose build --no-cache
```

---

## 🐛 Troubleshooting

### Issue: DAG not loading

**Solution:**
```bash
# Check Python path
docker exec -it airflow-worker python -c "import sys; print(sys.path)"

# Verify PYTHONPATH in docker-compose.yaml
PYTHONPATH: /opt/airflow/dags
```

### Issue: Ollama model not found

**Solution:**
```bash
# Pull model manually
docker exec -it ollama ollama pull llama3.2:1b

# Verify
docker exec -it ollama ollama list
```

### Issue: Dashboard connection error

**Solution:**
```bash
# Check database credentials
docker exec -it core_sentiment_streamlit printenv | grep DB_

# Test connection
docker exec -it core_sentiment_streamlit python -c "
import psycopg
conn = psycopg.connect('dbname=core_sentiment_db user=dki host=core_sentiment_db')
print('✓ Connected')
"
```

### Issue: No data in dashboard

**Solution:**
1. Check if pipeline has run: Airflow UI → DAG Runs
2. Verify data exists:
```bash
docker exec -it core_sentiment-db psql -U dki -d core_sentiment_db -c "SELECT COUNT(*) FROM filtered_pageviews;"
```
3. Trigger manual run: Airflow UI → Trigger DAG

---

## 📈 Performance

- **Pipeline Duration**: ~30-45 minutes (depends on data size)
- **Raw Data Size**: ~10M rows/day (~2GB)
- **Filtered Data**: ~500-1000 rows/day (~50KB)
- **LLM Processing**: ~2-5 minutes (local inference)
- **Dashboard Load Time**: <2 seconds (with caching)

---

## 🔐 Security Notes

- **Never commit `.env`** to version control
- Use **app passwords** for Gmail (not main password)
- Rotate **Slack webhooks** periodically
- Set **strong database passwords**
- Limit **PostgreSQL exposure** (port 5434 only localhost)

---

## 🚧 Future Enhancements

- [ ] Sentiment analysis implementation
- [ ] Historical trend analysis
- [ ] Multi-language support
- [ ] API endpoint for external access
- [ ] Machine learning model for classification
- [ ] Automated anomaly detection
- [ ] Mobile-responsive dashboard