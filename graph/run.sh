#!/usr/bin/bash

THIS_DIR=$(cd "$(realpath "$(dirname "${BASH_SOURCE[0]}")")" && pwd)
cd $THIS_DIR

spark-submit \
    --master yarn \
    --deploy-mode client \
    --executor-memory 4G \
    --num-executors 10 \
    --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 \
    main.py
