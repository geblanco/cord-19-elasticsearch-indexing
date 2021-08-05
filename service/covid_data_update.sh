#!/bin/bash

base_dir="${1:-$HOME}"
work_dir="${base_dir}/cord-19-elasticsearch-indexing"
src_volume="$(basename $work_dir)_data11"
dst_volume="$(basename $base_dir)_data01"

index_data(){
  local dir=$1
  cd $dir
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
}

start_instance(){
  cd $1;
  docker-compose up -d
}

clean_instance(){
  local instance_dir=$1; shift;
  local instance_volume=$1; shift;
  cd $instance_dir
  # stop original container, remove container and volume
  docker-compose stop elastic-search
  yes | docker-compose rm elastic-search
  docker volume inspect $instance_volume > /dev/null 2>&1
  if [[ "$?" -eq 0 ]]; then
    yes | docker volume rm $instance_volume
  fi
}

clone_volume(){
  # taken from docker_clone_volume.sh
  echo "Copying data from source volume \"$1\" to destination volume \"$2\"..."
  docker run --rm -v $1:/from -v $2:/to alpine ash -c "cd /from ; cp -av . /to"
}

clean_instance $work_dir $src_volume
start_instance $work_dir

# No errors allowed from here
set -e
index_data $work_dir
clean_instance $base_dir $dst_volume
clone_volume $src_volume $dst_volume
clean_instance $work_dir $src_volume
start_instance $base_dir
