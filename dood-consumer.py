from kafka import KafkaConsumer
from json import loads
from time import sleep

topic_name = 'my-test-topic'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    # enable_auto_commit=True,  # no consume
    enable_auto_commit=False,
    group_id='my-group-id',
    consumer_timeout_ms=1000,  # enable closing after 1 s
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

unique_ids = [];
all_ids = [];

# iterate the consumer, write unique uids to array.
# finally write out the number of the uniques (= lenth of array)

for event in consumer:
    event_data = event.value

    thisuid = event_data['uid']
    all_ids.append(thisuid)

    if thisuid not in unique_ids:
      unique_ids.append(thisuid) 
    

consumer.close()
print("all uids: " + str(len(all_ids)))
print("unique uids: " + str(len(unique_ids)))

print("consumer closed")