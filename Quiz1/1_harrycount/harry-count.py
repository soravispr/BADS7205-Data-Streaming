from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['streams-wordcount-output'])

import time

start_time = time.time()
harry = 0
value = 0

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    

    kvalue = msg.key().decode("utf-8")
    if kvalue == 'harry':
        value = msg.value()[-1]

    if time.time() - start_time >= 10:
        if harry == value:
            display_value = 0
        else:
            display_value = value - harry
            harry = value
        print('The name "Harry" was mentioned', display_value, 'during these past 5 sec')
        start_time = time.time()
    