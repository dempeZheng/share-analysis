#!/bin/sh
HDFS_HOST=hdfs://192.168.9.32
SPARK_HOME=


if [ ! -n "$HDFS_HOST" ]; then
    echo "you shuld set hdfs first"
else
    exec $SPARK_HOME/bin/spark-submit
fi