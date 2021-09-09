from confluent_kafka import Consumer
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['streams-tfidf-output'])
print("Start consuming...")

page_num = 1
while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    value = str(msg.value())
   
    vectorizer = TfidfVectorizer(stop_words='english')
    X = vectorizer.fit_transform([value])

    feature_array = np.array(vectorizer.get_feature_names())
    tfidf_sorting = np.argsort(X.toarray()).flatten()[::-1]

    n = 3
    top_n = feature_array[tfidf_sorting][:n]
    
    print(f'Page {page_num} - Most important word:')
    for i,n in enumerate(top_n):
        print(f'{i+1}. {n}')
    print('\n')
    page_num+=1

    #k = dict(zip(vectorizer.get_feature_names(), X.toarray()[0]))

c.close()

