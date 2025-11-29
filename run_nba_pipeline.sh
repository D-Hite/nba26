#!/usr/bin/env bash
set -euo pipefail

# --- SETTINGS ---
PROJECT_ROOT="/Users/dhite/Documents/GitHub/nba26"
VENV="$PROJECT_ROOT/.venv"
LOGFILE="$PROJECT_ROOT/pipeline.log"

# --- NAVIGATE TO PROJECT ROOT ---
cd "$PROJECT_ROOT"

# --- ACTIVATE uv VENV ---
# This ensures `python -m` resolves utils/ properly
source "$VENV/bin/activate"

# # --- ENV VARIABLES ---
# # Add Snowflake password for SQLMesh + ingestion
# # (Or export this in your shell instead)
# export SNOWFLAKE_PASSWORD="YOUR_REAL_SNOWFLAKE_PASSWORD_HERE"

echo "--- Starting NBA pipeline: $(date) ---" | tee -a "$LOGFILE"

# --- 1. RUN INGESTION ---
echo "Running ingestion script..." | tee -a "$LOGFILE"
python -m scripts.update_snowflake   2>&1 | tee -a "$LOGFILE"

# --- 2. RUN SQLMESH PLAN/RUN ---
echo "Running SQLMesh transforms..." | tee -a "$LOGFILE"
cd sqlmesh

# Auto-apply plan, no interactive prompt
sqlmesh plan main --auto-apply   2>&1 | tee -a "$LOGFILE"
sqlmesh run main                 2>&1 | tee -a "$LOGFILE"

echo "--- Pipeline finished: $(date) ---" | tee -a "$LOGFILE"
