#!/usr/bin/env bash
set -euo pipefail

LOG="logs/tenk_scrape_$(date +%Y%m%d).log"

echo "==================================================" | tee -a "$LOG"
echo "[START] $(date) | Host=$(hostname) | PWD=$(pwd)" | tee -a "$LOG"
echo "[LOG]   $LOG" | tee -a "$LOG"
echo "==================================================" | tee -a "$LOG"

# Infinite restart loop
while true; do
  echo "--------------------------------------------------" | tee -a "$LOG"
  echo "[RUN ] $(date) Starting scraper..." | tee -a "$LOG"

  # Run your job (idempotent because --skip-if-exists)
  uv run python -u -m src.fetch_10k_html_batch \
    --input gs://sec-financials-edgar/edgar_idx_files/filtered/company_index_10k.parquet \
    --output-prefix gs://sec-financials-edgar/edgar_10k_html/parts \
    --user-agent "NareshChethala (nc623367@wne.edu)" \
    --delay 2 \
    --retry-limit 2 \
    --checkpoint-every 25 \
    --skip-if-exists \
    >> "$LOG" 2>&1

  EXIT_CODE=$?

  echo "[EXIT] $(date) Exit code=$EXIT_CODE" | tee -a "$LOG"
  echo "[WAIT] Sleeping 60 seconds before restart..." | tee -a "$LOG"
  sleep 60
done
