from kafka import KafkaConsumer
from json import loads
from time import sleep
from datetime import datetime

consumer = KafkaConsumer(
    'my_1000000_topic',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    # enable_auto_commit=True, 
    enable_auto_commit=False,
    group_id='my-group-id2',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print('start')
counter = 0
for event in consumer:
    counter = counter+1
    event_data = event.value
    thisuid = event_data['uid']
    thisTimestamp = event_data['ts']

    print(thisuid + ' : '+ datetime.utcfromtimestamp(thisTimestamp).strftime('%Y-%m-%d %H:%M:%S'))
    
print('counter', counter)