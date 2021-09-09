from confluent_kafka import Producer
import time
import os
import re

p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292'})
exclude_words = ['Mr', 'Mrs']
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {}'.format(msg.value().decode('utf-8')))

file1 = open('./book.txt', 'r', encoding="utf8") 
#page = []
#Lines = file1.readlines()
data = file1.read()

pages = data.split('Page |')

for page in pages:
    #page=page.replace('\n', '').replace('“','').replace('”','').replace(',','')
    page=re.sub('[^\w\s]','',page)
    
    for e in exclude_words:
        page=page.replace(e, '')
  
    p.produce('streams-tfidf-input', page, callback=delivery_report)

    os.system('pause')
'''
for data in Lines:

    p.poll(0)
    #sendMsg = data.encode().decode('utf-8').strip('\n')
    sendMsg = data
    if sendMsg.find('Page |', 0,6) != -1:
        print(''.join(page))
        p.produce('streams-tfidf-input', ''.join(page) , callback=delivery_report)
        page = []
        time.sleep(1)
    else:
        page.append(sendMsg)
'''

p.flush()
