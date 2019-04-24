#!/bin/bash
spark-submit \
  --class com.carrefour.phenix.Application \
  --master local[2] \
  --deploy-mode client \
  --files ../jars/transaction-aggregator-with-dependencies.jar \
  --conf spark.executor.memory=512m \
  --conf spark.driver.memory=512m \
  --conf spark.executor.cores=2 \
  ../jars/transaction-aggregator-with-dependencies.jar \
  -i ../sample/input \
  -o ../sample/output \
  -d 20170514