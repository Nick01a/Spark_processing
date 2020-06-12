
import sys
import os
import time
from datetime import datetime
from producer import send_all
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
from pyspark.sql import SparkSession
import json


def connect_to_spark():
    return SparkSession.builder.master("local[*]").appName("spark").getOrCreate()


os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar pyspark-shell'

sc = connect_to_spark().sparkContext

ssc = StreamingContext(sc, 60)
kvs = KafkaUtils.createDirectStream(ssc, ["memory"], {"metadata.broker.list": "localhost:9092"})


all_topics = ["Computer programming", "Big Data", "Machine Learning", "Python", "Java", "Web Development"]


def filt_1(record):
    task_1 = dict()
    record_json = json.loads(record)


    if (record_json["group"]["group_country"]=="us"):
        task_1["event"]={"event_name":record_json["event"]["event_name"], "event_id":record_json["event"]["event_id"],
                         "time" : time.ctime(record_json["event"]["time"]/1000)}
        task_1["group_city"]= record_json["group"]["group_city"]
        task_1["group_country"]=record_json["group"]["group_country"]
        task_1["group_id"] = record_json["group"]["group_id"]
        task_1["group_name"] = record_json["group"]["group_name"]
        task_1["group_state"] = record_json["group"]["group_state"]

        with open("results1.json", 'a', encoding='utf-8') as f:
            f.write(json.dumps(task_1, ensure_ascii=False, sort_keys=True, indent=4))

        send_all("US-meetups", str(task_1))


def filt_4(record):
    task_4 = dict()
    record_json = json.loads(record)

    if (record_json["group"]["group_country"] == "us"):
        contains = False
        all_topics = []
        for i in record_json["group"]["group_topics"]:
            if i["topic_name"] in all_topics:
                contains = True

            all_topics.append(i["topic_name"])
        if contains:
            task_4["event"] = {"event_name": record_json["event"]["event_name"],
                               "event_id": record_json["event"]["event_id"],
                               "time": time.ctime(record_json["event"]["time"] / 1000)}
            task_4["group_city"] = record_json["group"]["group_city"]
            task_4["group_country"] = record_json["group"]["group_country"]
            task_4["group_id"] = record_json["group"]["group_id"]
            task_4["group_name"] = record_json["group"]["group_name"]
            task_4["group_state"] = record_json["group"]["group_state"]

            with open("results4.json", 'a', encoding='utf-8') as f:
                f.write(json.dumps(task_4, ensure_ascii=False, sort_keys=True, indent=4))

        send_all("Programming-meetups", str(task_4))

def f1(x):
    x.foreach(lambda i: filt_1(i[1]))

def f3(x):
    task_3 = {"cities": []}
    lst = x.first()

    for i in lst:
        task_3["cities"].append(i[1])
        date = time.ctime(i[0] / 1000)
        task_3["month"] = date.split()[1]
        task_3["day_of_month"] = date.split()[2]
        task_3["hour"] = date.split()[3].split(":")[0]
        task_3["minute"] = date.split()[3].split(":")[1]


    with open("results3.json", 'a', encoding='utf-8') as f:
        f.write(json.dumps(task_3, ensure_ascii=False, sort_keys=True, indent=4))

    send_all("US-cities-every-minute", str(task_3))


def f4(x):
    x.foreach(lambda i: filt_4(i[1]))

# kvs.foreachRDD(lambda x: f1(x))
kvs.foreachRDD(lambda x: f4(x))



winds = kvs.window(windowDuration=60, slideDuration=60).filter(lambda x: json.loads(x[1])["group"]["group_country"] == "us").\
    map(lambda x: [json.loads(x[1])["event"]["time"], json.loads(x[1])["group"]["group_city"]]).\
    glom()
# winds.foreachRDD(lambda x: f3(x))



ssc.start()
ssc.awaitTermination()

