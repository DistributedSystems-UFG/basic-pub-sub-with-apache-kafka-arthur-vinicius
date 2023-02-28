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
<<<<<<< HEAD
        msg = 'My ' + str(random.randint(1,1000)) + 'st message for topic ' + topic
=======
        msg = 'Message id number ' + str(random.randint(1,1000)) + ' for topic ' + topic
>>>>>>> 761158e7ed7b515c13ded3ee6d89807475b62c1a
        print ('Sending message: ' + msg)
        producer.send(topic, value=msg.encode())
    time.sleep(1)

<<<<<<< HEAD
    producer.flush()
=======
    producer.flush()
>>>>>>> 761158e7ed7b515c13ded3ee6d89807475b62c1a
