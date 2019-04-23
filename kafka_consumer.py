import json
import datetime

from kafka import KafkaConsumer
consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092', 
    value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(['posts_clean'])

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.encoder.mapping[list] = session.encoder.cql_encode_list_collection

query = SimpleStatement("""
    INSERT INTO pw_project.posts (
        id, 
        accepted_answer_id, 
        answer_count, 
        body,
        comment_count, 
        creation_date, 
        favorite_count, 
        last_activity_date, 
        last_edit_date, 
        last_editor_user_id, 
        owner_user_id, 
        post_type_id, 
        score,
        title,
        tags, 
        view_count)
    VALUES (
        %(id)s, 
        %(accepted_answer_id)s, 
        %(answer_count)s, 
        %(body)s, 
        %(comment_count)s, 
        %(creation_date)s, 
        %(favorite_count)s, 
        %(last_activity_date)s, 
        %(last_edit_date)s, 
        %(last_editor_user_id)s, 
        %(owner_user_id)s, 
        %(post_type_id)s, 
        %(score)s, 
        %(title)s, 
        %(tags)s, 
        %(view_count)s)
    """, consistency_level=ConsistencyLevel.ONE)

count = 0
for msg in consumer:
    msg_clean = msg.value
    session.execute(query, msg_clean)
    count = count + 1
    print('R ' + datetime.datetime.now().strftime("%H:%M:%S:%f") + 
          ' R=' + '{:04d}'.format(count) +
          ' id=' + str(msg_clean.get('id')))