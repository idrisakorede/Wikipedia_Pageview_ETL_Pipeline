#!/bin/bash
set -e

echo "Starting local Airflow setup..."

# Activate virtual environment
if [ -d "venv" ]; then
  source venv/bin/activate
else
  echo "❌ Virtual environment not found! Run scripts/setup.sh first."
  exit 1
fi

# Export environment variables
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$(pwd)

# Initialize Airflow database
airflow db init
echo "✅ Airflow database initialized."

# Create default user (optional, only first time)
airflow users create \
  --username admin \
  --firstname Idris \
  --lastname Ibrahim \
  --role Admin \
  --email admin@example.com \
  --password admin || true

# Start Airflow scheduler in background
airflow scheduler -D
echo "✅ Airflow scheduler started."

# Start Airflow webserver
airflow webserver -p 8080