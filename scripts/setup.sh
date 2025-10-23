#!/bin/bash
set -e

echo "Setting up environment for Wikipedia Airflow Pipeline..."

# Create and activate virtual environment
if [ ! -d "venv" ]; then
  python3 -m venv venv
  echo "✅ Virtual environment created."
fi

# Activate environment
source venv/bin/activate
echo "✅ Virtual environment activated."

# Upgrade pip
pip install --upgrade pip

# Install dependencies
if [ -f "requirements.txt" ]; then
  pip install -r requirements.txt
  echo "✅ Dependencies installed successfully."
else
  echo "❌ requirements.txt not found!"
  exit 1
fi

# Create required folders
mkdir -p data logs
echo "📂 Data and logs folders ready."

echo "Setup complete! You can now run the pipeline using scripts/run_local.sh"