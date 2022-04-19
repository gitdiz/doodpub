import subprocess
import time

starttime = time.time()
tick = 0

while True:
    tick += 1
    print("tick" + str(tick))
    subprocess.getstatusoutput(f'./data-generator-linux -c 10  -r 1000 -n 100000 \
        | docker exec -i ksql_kafka_1 kafka-console-producer --broker-list localhost:9092 --topic my_test_topic')
    time.sleep(0.5)

# s = subprocess.getstatusoutput(f'ps -ef | grep python3')
# s = subprocess.getstatusoutput(f'./data-generator-linux -c 1  -r 1000 -n 100000')
# s = subprocess.getstatusoutput(f'./data-generator-linux -c 1  -r 1000 -n 100000 | docker exec -i ksql_kafka_1 kafka-console-producer --broker-list localhost:9092 --topic my_test_topic')
# print(s)
