#!/bin/bash
mkdir -p logs
find ./logs/ -regex ".*/consumer_producer_.*.txt" -delete

count_process=$1

echo "Starting $count_process process(es)"

for i in $(seq 1 $count_process)
do
    log_file="./logs/consumer_producer_$i.txt"
    echo "Start process $i with logs to $log_file"
    python -u ./kafka_consumer_producer.py > $log_file 2>&1 &
done