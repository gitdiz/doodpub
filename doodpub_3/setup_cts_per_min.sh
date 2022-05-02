
# raw data topic from data generator tool
# create topic 1 mill rows  in docker
docker exec -ti ksql_kafka_1 kafka-topics  --create --bootstrap-server localhost:9092  --replication-factor 1 --partitions 1 --topic my_1000000_topic


# create topics cts per min in docker 
docker exec -ti ksql_kafka_1 kafka-topics  --create --bootstrap-server localhost:9092  --replication-factor 1 --partitions 1 --topic my_uid_per_min_topic

# feed the topic in docker from data tool. create 1 million messages and record to the topic my_1000000_topic
./data-generator-linux -c 1000000  -r 1000 -n 100000 | docker exec -i ksql_kafka_1 kafka-console-producer --broker-list localhost:9092 --topic my_1000000_topic

#########
# find distinct uid counts per minute and write to a new topic  'my_uid_per_min_topic'
# the file is manual triggererd and can be autotriggered (depending on the calculaton time)
############
python3 dood-consumer-producer.py
# when executed, updated topic 

########
# add 10 new rows per second from data tool to 'my_1000000_topic'
######## 
python3 datatoolFromPy.py
# expected output: 
# dz@T470:~/kafka/datatool$ python3 datatoolFromPy.py 
# tick1
# tick2 ...

########
# watch/print out the 'my_1000000_topic' stream 
######## 
python3 myTopicConsumer.py
# expected output: 
# dz@T470:~/kafka/ksql/myFiles$ python3 my1000000TopicConsumer.py 
# start
# 06d7f22f68a0270d660 : 2016-07-11 13:39:44
# 0a78e22df9e3494bf09 : 2016-07-11 13:39:44
# 66050a56934d8fe921e : 2016-07-11 13:39:44
# c69e11cec5ac79eb00a : 2016-07-11 13:39:44
# c4b7eadbd831b4f23dc : 2016-07-11 13:39:44
# 016f8be6a3699d8445e : 2016-07-11 13:39:44 ...


########
# watch/print out the 'uid per min' stream 
######## 
python3 uid_per_min_consumer.py
# expected output: 
# dz@T470:~/kafka/ksql/myFiles$ python3 uid_per_min_consumer.py
# {'2016-07-11 13:39:00': 150}
# {'2016-07-11 13:39:00': 20}




##########  useful kafka commands
#### list topics in container  
# [appuser@c4c7881b96c6 ~]$ kafka-topics --bootstrap-server localhost:29092 --list


####  delete topic in container 
#[appuser@c4c7881b96c6 ~]$ kafka-topics --bootstrap-server localhost:29092 --delete --topic my_1000000_topic

#### count messages in topic in container
#[appuser@c4c7881b96c6 ~]$ kafka-run-class kafka.tools.GetOffsetShell  --broker-list localhost:9092   --topic m y_1000000_topic  --time -1
