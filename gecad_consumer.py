from confluent_kafka import Consumer
from confluent_kafka.cimpl import KafkaError, KafkaException
import sys
import re
import json
# from scapy.all import *
from datetime import datetime
from elasticsearch import Elasticsearch
import threading

#Kafka Consumer instance
def consumer(networkName, topic):

    es = Elasticsearch('192.168.5.71:9200')
    conf = {'bootstrap.servers': "192.168.5.41:9092", 'group.id': "demo", 'auto.offset.reset': 'smallest'}
    consumer = Consumer(conf)
    topics = [topic]
    consumer.subscribe(topics)
    running = True

    try:
        consumer.subscribe(topics)

        msg_count = 0
        nd_count = 0
        json_object = ""
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # msg_process(msg)
                # print(msg.value().decode("utf-8"))
                # messageStr = (msg.value())
                if "_index" in msg.value().decode("utf-8"):
                    json_object = "{"
                    nd_count +=1
                if nd_count > 0:
                    json_object += (msg.value().decode("utf-8"))
                if msg.value().decode("utf-8").strip() == ",":
                # wrpcap('filtered.pcap', msg.value(), append=True)
                    with open('json_file_pretty','a', encoding='utf-8') as outfile:
                #  json.dump(messageStr, fp)
                        if json_object != "":
                            x = json.loads(json.dumps(json_object, sort_keys=True, indent=4))
                            print(x)
                            print(networkName)
                            json.dump(x, outfile)
                    json_object = "" 
                    nd_count = 0
                #  doc = {
                #  'text': messageStr,
                #  'timestamp': datetime.now(),
                # }
                # es.index(index="test-index", body=doc, refresh='wait_for')
                msg_count += 1
                # if msg_count % MIN_COMMIT_COUNT == 0:
                # consumer.commit(async=False)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

#Create 3 threads as follows
try:
    t1=threading.Thread(target=consumer, args=("Core Network", 'AMC_core_capture'))
    t2=threading.Thread(target=consumer, args=("DMZ", 'AMC_dmz_capture'))
    t3=threading.Thread(target=consumer, args=("Medical Imaging Network", 'AMC_med_capture'))
    t1.start()
    t2.start()
    t3.start()
except:
   print("Error: unable to start thread")
