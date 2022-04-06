#!/bin/bash


# docker install
wget -qO- https://get.docker.com/ | sh

# docker-compose install
pip3 install docker-compose

# python api for kafka
pip3 install kafka-python

# get kafka for docker
git clone https://github.com/wurstmeister/kafka-docker.git 


cd kafka-docker 

# run docker 
docker-compose -f docker-compose-expose.yml up

#generated json test data with the tool (100 sets) stream100.jsonl 
# ./data-generator-linux -c 100 -o  stream100.jsonl -r 1000 -n 100000

# compress it
gzip stream100.jsonl 

# copy stream data into container
# check correct docker id with docker ps 

docker cp stream100.jsonl.gz kafka-docker_kafka_1:stream100.jsonl.gz


# create a topic
docker exec -ti  kafka-docker_kafka_1 /opt/kafka/bin/kafka-topics.sh --create --topic my-test-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092 


# feed topic in the kafka container from the gzipped json file
docker exec -ti kafka-docker_kafka_1  sh -c 'gunzip -c  stream100.jsonl.gz  | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-test-topic'

#count messages in topic
docker exec -ti  kafka-docker_kafka_1 /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell  --broker-list localhost:9092 --topic  my-test-topic 
> my-test-topic:0:100

# feed kafka topic again  in the container to get doubles
docker exec -ti kafka-docker_kafka_1  sh -c 'gunzip -c  stream100.jsonl.gz  | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-test-topic'

# get data fo the topic, read uid, and count uniques out with python script dood-consumer.py
# run the consumer
$ python3 dood-consumer.py 

#prints out something like 
# dz@T470:~/kafka/kafka-docker$ python3 dood-consumer.py 
# all uids: 400
# unique uids: 100
# consumer closed


# uids look like 
# 430a410a5514ee173dd
# ca2908a238d4df47e85
# bbf9354a1357d4a0ab8
# 5763cd130dc51c0ea01
# d602f508521744d549f
# 6cbfc15bb1bc04e9bb1

# just in case reset offset of a consumer group if consumption of topics was enabled
# docker exec -ti  kafka-docker_kafka_1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-group-id3 --reset-offsets --to-earliest --all-topics --execute

#shutdown docker if wanted
# docker-compose stop
