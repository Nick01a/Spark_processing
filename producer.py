from json import dumps
from kafka import KafkaProducer
import json
import requests

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

def send_all(topic, message):
    producer.send(topic, value=message)


def send_to_memory():
    r = requests.get('http://stream.meetup.com/2/rsvps', stream=True)

    if r.encoding is None:
        r.encoding = 'utf-8'



    for line in r.iter_lines(decode_unicode=True):
        if line:
            producer.send("memory", value=json.loads(line))


if __name__ == "__main__":
    send_to_memory()








