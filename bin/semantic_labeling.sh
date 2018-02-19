#!/usr/bin/env bash

DATASET_DIR=$1
OUTPUT_DIR=$2
shift 2

docker run --rm --net=host \
    -v $DATASET_DIR:/home/data/datasets \
    -v $OUTPUT_DIR:/home/output \
    isi/semantic-labeling python -m main.api $@
