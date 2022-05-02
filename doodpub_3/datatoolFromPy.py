import subprocess
import time

starttime = time.time()
tick = 0

## writes 10 messages per 1 sec tick from the data tool to a topic in docker

while True:
    tick += 1
    print("tick" + str(tick))
    subprocess.getstatusoutput(f'./data-generator-linux -c 10  -r 1000 -n 100000 \
        | docker exec -i ksql_kafka_1 kafka-console-producer --broker-list localhost:9092 --topic my_1000000_topic')
    time.sleep(1)

