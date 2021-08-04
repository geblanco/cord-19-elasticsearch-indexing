#!/bin/bash

base_dir="${1:-$HOME}"
work_dir="${base_dir}/cord-19-elasticsearch-indexing"

# create a new ES instance
cd $work_dir
docker-compose up -d
source .venv/bin/activate
# index data
python dl_cord19.py \
  --download \
  --scrape_latest \
  --index \
  -a 0.0.0.0 \
  -p 9201 \
  --batch_size 50 \
  --incl_abs

# stop container, no more data writing
docker-compose stop elastic-search

# stop original ES and remove container
cd $base_dir
docker-compose stop elastic-search
yes | docker-compose rm elastic-search

# remove orignal ES volume
[[ "$(docker volume ls | grep qaservers_data01 | wc -l)" -gt 0 ]] && yes | docker volume rm qaservers_data01

# clone volume and start fresh ES
docker_clone_volume.sh cord-19-elasticsearch-indexing_data11 qaservers_data01
docker-compose up -d
