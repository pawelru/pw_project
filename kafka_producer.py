import sys
import json
import datetime
import time

step = int(sys.argv[1])
count_step = int(sys.argv[2])

from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    buffer_memory=2000*10**6,
    max_block_ms=60000*10,
    connections_max_idle_ms=2*60*1000,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

count_sent = 0

file = open('./data/Posts.xml')

for i, line in enumerate(file):
    if i % count_step == (step - 1):
        time.sleep(0.01)
        producer.send('posts_all', line)
        count_sent = count_sent + 1
        print('S ' + datetime.datetime.now().strftime("%H:%M:%S:%f") + 
              ' S=' + '{:04d}'.format(count_sent))