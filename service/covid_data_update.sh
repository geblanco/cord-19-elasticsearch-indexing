#!/bin/bash

base_dir="${1:-$HOME}/cord-19-elasticsearch-indexing"
echo "Working on $base_dir"
cd $base_dir
source .venv/bin/activate
python dl_cord19.py \
  --download \
  --scrape_latest \
  --index \
  -a 0.0.0.0 \
  -p 9200 \
  --batch_size 50
