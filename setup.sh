#!/bin/zsh

set -e  # Exit immediately if a command exits with a non-zero status

# Define environment name
VENV_DIR=".venv"

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment..."
  python3 -m .venv $VENV_DIR
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Upgrade pip and install requirements
echo "Installing requirements..."
pip install --upgrade pip
pip install -r requirements.txt

# Run the ETL script
echo "Running ETL job..."
python3 batch_jobs/stock_etl.py