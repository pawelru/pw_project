#!/bin/bash
mkdir -p logs
find ./logs/ -regex ".*/consumer_.*.txt" -delete

count_process=$1

echo "Starting $count_process process(es)"

for i in $(seq 1 $count_process)
do
    log_file="./logs/consumer_$i.txt"
    echo "Start process $i with logs to $log_file"
    python -u ./kafka_consumer.py > $log_file 2>&1 &
done