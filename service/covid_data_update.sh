#!/bin/bash

base_dir="${1:-$HOME}"
our_dir="${base_dir}/cord-19-elasticsearch-indexing"
# stop elastic search and remove container
cd $base_dir
echo "Stopping elastic search container"
docker-compose stop elastic-search
yes | docker-compose rm elastic-search
[[ "$(docker volume ls | grep qaservers_data01 | wc -l)" -gt 0 ]] && yes | docker volume rm qaservers_data01
docker-compose up -d
echo "Working on $our_dir"
cd $our_dir
source .venv/bin/activate
python dl_cord19.py \
  --download \
  --scrape_latest \
  --index \
  -a 0.0.0.0 \
  -p 9200 \
  --batch_size 50 \
  --incl_abs
