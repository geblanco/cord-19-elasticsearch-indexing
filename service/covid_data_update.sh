#!/bin/bash

base_dir="${1:-$HOME}/cord-19-elasticsearch-indexing"
source .venv/bin/activate
python dl_cord.py \
  --download \
  --scrape_latest \
  --index \
  -a 0.0.0.0 \
  -p 9201 \
  --batch_size 50
