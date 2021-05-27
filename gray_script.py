from confluent_kafka import Producer
import json
import socket

conf = { 'bootstrap.servers' : '192.168.5.41:9092', 'client.id' : socket.gethostname() }
producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

producer.produce('machine_learning_engine', key='key1', value='Machine learning processed data', callback=acked)

producer.flush()
producer.poll(1)
