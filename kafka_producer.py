import sys
import json
import datetime
import subprocess

range_from = int(sys.argv[1])
range_to = int(sys.argv[2])

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

count_sent = 0

for i in range(range_from, range_to):
    bashCommand = 'sed -n ' + str(i) + 'p < data/Posts.xml'
    msg = subprocess.run(bashCommand.split(), capture_output=True).stdout.decode("utf-8") 
    producer.send('posts-all', msg)
    count_sent = count_sent + 1
    print('S ' + datetime.datetime.now().strftime("%H:%M:%S:%f") + 
          ' S=' + '{:04d}'.format(count_sent))