#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 10:16:58 2019

@3301
"""


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import time
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import HiveContext

sc = SparkContext(appName="PysparkNotebook")

sqlContext = HiveContext(sc)
ssc = StreamingContext(sc, 30) ###to_change
kafkaStream = KafkaUtils.createStream(ssc,
                                      'quickstart.cloudera:2181', 
                                      'spark-streaming', 
                                      {'twitter_feed':1})
# funciton to convert json into tuple
def extract_user(raw):
    return ((raw["user"])["id"],
     (raw["user"])["name"],
     (raw["user"])["screen_name"],
     (raw["user"])["description"],
     (raw["user"])["location"],
     (raw["user"])["created_at"],
     (raw["user"])["friends_count"],
     (raw["user"])["followers_count"],
     (raw["user"])["statuses_count"],
     (raw["user"])["verified"],
     time.time()
     )
    
#schema definition for user table in hive
user_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("screen_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("location", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("friends_count", LongType(), True),
    StructField("followers_count", LongType(), True),
    StructField("statuses_count", LongType(), True),
    StructField("verified", StringType(), True),
    StructField("s_created_at", DoubleType(), True),
])
    
def processrdd(rdd):
    data_df = sqlContext.createDataFrame(rdd,schema=user_schema) ## mapping tuple with schema 
    data_df.write.mode('append').saveAsTable("default.twitter_user") #appending data to hive table

parsed = kafkaStream.map(lambda v: json.loads(v[1].decode('utf-8')))  ##called as DStreams

parsed.map(lambda x : extract_user(x)).foreachRDD(processrdd) ## converting Dstreams into RDD


ssc.start() #kafka streams application will start
ssc.awaitTermination()

