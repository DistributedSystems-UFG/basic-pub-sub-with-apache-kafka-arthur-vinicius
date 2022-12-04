from kafka import KafkaProducer
from const import *
import sys
import time
import random

try:
    topics = sys.argv[1].split(',')
except:
    print ('Usage: python3 producer <topic_name>')
    exit(1)
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
for i in range(100):
    for topic in topics:
        msg = 'Message id number ' + str(random.randint(1,1000)) + ' for topic ' + topic
        print ('Sending message: ' + msg)
        producer.send(topic, value=msg.encode())
    time.sleep(1)

    producer.flush()