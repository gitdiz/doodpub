from kafka import KafkaConsumer
from json import loads, dumps
from time import sleep, time
from datetime import datetime
from itertools import groupby
from operator import itemgetter



def runTopicAnalyzer():
    topic_name = 'my_1000000_topic'

    # create consumer with auto commit

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,  # do consume
        # enable_auto_commit=False, # no consume
        group_id='my-group-id1',
        consumer_timeout_ms=1000,  # enable closing after 1 s
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    # some array to write out uid and ts data
    unique_ids = []
    all_ids    = []
    all_ts     = []
    all_ts_m   = []

    counter = 0  # all messages in topic
    start   = datetime.now()
    print('start:', end=' ') ,  # end is the line end! 
    print (start.strftime("%Y-%m-%d %H:%M:%S"))

    # iterate the consumer, write unique uids to array.
    # finally write out the number of the uniques (= lenth of array)

    for event in consumer:

        if (counter%10000 == 0):   # write something out
            print(counter, end='\r')
        counter = counter+1
        event_data = event.value

        thisuid = event_data['uid']
        thisTimestamp = event_data['ts']
        thisRealTimestamp = event_data['ts'] - 5 # recorded timestamp is maximal 5sec delayed. shift it back

        all_ids.append(thisuid)   ## write data of interest (uid,ts)to arrays 
        # all_ts.append(thisTimestamp)
        all_ts.append(thisRealTimestamp)

        # if counter >= 1000000:
        #    break

    consumer.close()  
    print("consumer closed")

    # print() # empty line for clear json

    def to_minute(ts):
        return (int(ts /60))*60  # to minute

    all_ts_m = list(map(to_minute,all_ts ))

    # zipped_list= list(zip(all_ids,all_ts))
    zipped_list_min= list(zip(all_ids,all_ts_m))

    # unique_ziplist = set(zipped_list)
    unique_ziplist_min = set(zipped_list_min)

    groups               = []
    # uniquekeys           = []
    distinct_uid_pm      = []
    # distinct_uid_pm_json = []

    # group the 2-tuple by minute, sort by time
    for k, g in groupby(sorted(unique_ziplist_min, key=lambda k: k[1]) , itemgetter(1)):

        cts = len(list(g))     # the minute group has all distinct uids, get the count
        groups.append(cts)     
        # uniquekeys.append(k)
        thisObj = {datetime.utcfromtimestamp(k).strftime('%Y-%m-%d %H:%M:%S'): cts}
        distinct_uid_pm.append(thisObj)


    minute_bins = set(all_ts_m)
    unique_ids= set(all_ids)

    print("groups : "            + str(list(groups)))
    print("all uids: "           + str(len(all_ids)))
    print("unique uids: "        + str(len(unique_ids)))
    print("unique_ziplist_min: " + str(len(unique_ziplist_min)))
    print("minute_bins:"         + str(len(minute_bins)) +" " +  str(minute_bins))

    print('counter:', counter)
    end = datetime.now()
    print('end:' , end=' '), 
    print (end.strftime("%Y-%m-%d %H:%M:%S"))
    print ('timediff: ', end-start)


    ###################
    ## send to cts per minute topic my_uid_per_min_topic
    #####################
    from time import sleep
    from json import dumps
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    j = 0
    for msg in distinct_uid_pm:
        data = msg 
        producer.send('my_uid_per_min_topic', value=data)
        sleep(0.2)
        j = j + 1




tick = 0

while True:
    tick += 1
    print("#####   analyzer tick " + str(tick)+ "  ###########")
    runTopicAnalyzer()
    sleep(60)