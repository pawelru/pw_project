#!/bin/bash
rm -rf logs
mkdir logs

echo $(ls)

count_lines=$(wc -l < ./data/Posts.xml)
echo $count_lines

count_process=$1
echo $count_process

batch_size=$((count_lines / count_process))
echo $batch_size

echo "Starting $count_process process(es)"

for i in $(seq 1 $count_process)
do
    range_start=$(((i - 1) * batch_size + 1))
    range_start=$(( range_start > 3 ? range_start : 3 ))
    range_stop=$((i * batch_size))
    log_file="./logs/producer_$i.txt"
    echo "Process $i range from $range_start to $range_stop with logs to $log_file"
    python -u ./kafka_producer.py $range_start $range_stop > $log_file 2>&1 &
done