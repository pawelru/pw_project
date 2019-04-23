import json
import datetime
import xml.etree.ElementTree
from bs4 import BeautifulSoup
import re

def convert(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def check(d, name, value):
    if name not in d:
        d[name] = value

def correct(input):
    try:
        temp = dict(xml.etree.ElementTree.fromstring(input).items())
    except:
        return None
    
    if temp.get('PostTypeId') != '1':
        return None
    
    temp['Tags'] = temp['Tags'].replace('><', '_#_').replace('<', '').replace('>', '').split('_#_')
    
    body_clear = BeautifulSoup(temp['Body'], features="html.parser")
    while body_clear.code is not None:
        body_clear.code.decompose()
    temp['Body'] = body_clear.getText().replace('\n', ' ')
    
    result = {}
    for key, value in temp.items():
        if not isinstance(value, (list,)):
            try:
                value = int(value)
            except ValueError:
                pass
        result[convert(key)] = value
    
    check(result, 'id', -1)
    check(result, 'accepted_answer_id', -1)
    check(result, 'answer_count', 0)
    check(result, 'body', "")
    check(result, 'comment_count', 0)
    check(result, 'creation_date', "")
    check(result, 'favorite_count', 0)
    check(result, 'last_activity_date', "")
    check(result, 'last_edit_date', "")
    check(result, 'last_editor_user_id', -1)
    check(result, 'owner_user_id', -1)
    check(result, 'post_type_id', -1)
    check(result, 'score', 0)
    check(result, 'tags', "")
    check(result, 'title', "")
    check(result, 'view_count', 0)
    return result




from kafka import KafkaConsumer
consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092', 
    group_id='test',
    auto_offset_reset='earliest',
    receive_buffer_bytes=16*2*10**6,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(['posts_all'])

from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))


count_received = 0
count_sent = 0

for msg in consumer:
    msg_clean = msg.value
    count_received = count_received + 1
    id_ = re.sub('".*', '', re.sub('.*?="', '', msg_clean, 1)).replace('\n', '')
    print('R ' + datetime.datetime.now().strftime("%H:%M:%S:%f") + 
          ' R=' + '{:04d}'.format(count_received) + 
          ' S=' + '{:04d}'.format(count_sent) +
          ' id=' + id_)
    msg_dict = correct(msg_clean)
    if msg_dict is not None:
        producer.send('posts_clean', msg_dict)
        count_sent = count_sent + 1
        print('S ' + datetime.datetime.now().strftime("%H:%M:%S:%f") + 
          ' R=' + '{:04d}'.format(count_received) + 
          ' S=' + '{:04d}'.format(count_sent) +
          ' id=' + id_)