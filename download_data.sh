#!/bin/bash

# Common utils
wget_cmd="wget -q --show-progress --limit-rate=1000M"

current_dir=$(dirname "$0")
source $current_dir/.env
out_dir=$DUMP_PATH
mkdir -p $out_dir

# Crossref Metadata
# Download the metadata via torrent crossref_2025.torrent in folder $DUMP_PATH
# sudo apt install transmission -y
transmission-cli -w $out_dir $current_dir/crossref_2025.torrent

