#!/bin/bash

# Common utils
wget_cmd="wget -q --show-progress --limit-rate=1000M"

source $(dirname -- "$0")/.env
out_dir=$DUMP_PATH
mkdir -p $out_dir

# Crossref Metadata
# Download the metadata via torrent crossref_2023.torrent in folder $DUMP_PATH
