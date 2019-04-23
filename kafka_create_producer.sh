#!/bin/bash
mkdir -p logs
find ./logs/ -regex ".*/producer_.*.txt" -delete

count_process=$1

echo "Starting $count_process process(es)"

for i in $(seq 1 $count_process)
do
    log_file="./logs/producer_$i.txt"
    echo "Start process $i with logs to $log_file"
    python -u ./kafka_producer.py $i $count_process > $log_file 2>&1 &
done