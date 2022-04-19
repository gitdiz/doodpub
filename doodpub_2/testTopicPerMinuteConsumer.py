from kafka import KafkaConsumer
from json import loads
from time import sleep
from datetime import datetime

# from io import BytesIO

consumer = KafkaConsumer(
    'DIST_UID_P_M_2',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-py-group2',
    # value_deserializer=lambda x: loads(x.decode('utf-8'))
    # value_deserializer=lambda x: loads(x)
)


for event in consumer:

    print(event)

    sleep(.2)
