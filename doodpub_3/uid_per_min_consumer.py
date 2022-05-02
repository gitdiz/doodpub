from kafka import KafkaConsumer
from json import loads
from time import sleep
from datetime import datetime


consumer = KafkaConsumer(
    'my_uid_per_min_topic',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='uid-per-min1',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

for event in consumer:
    event_data = event.value
    # thisuid = event_data['uid']
    # thisTimestamp = event_data['ts']
    print(event_data)
    # print(thisuid + ' : '+ datetime.utcfromtimestamp(thisTimestamp).strftime('%Y-%m-%d %H:%M:%S'))
    # Do whatever you want
    #print(event_data['uid'])
    sleep(.2)
