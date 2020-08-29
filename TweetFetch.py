# -*- coding: utf-8 -*-
"""
The code listens to the list of hashtags and fetches tweets. The received tweets
are in json which will be pushed to kafka topic.
@3301
"""

import tweepy
import time
from tweepy import OAuthHandler
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# Twitter configuration
consumer_key = 'vvB2wJsNUy26AcXWSX77rudsJ'
consumer_secret = 'IJR2yX5kwj5mkgUfn2uTAC7oRQzwAwu4WaUKHQ7fIizghLpL1W'
access_token = '99280962-Hs8SZPSa6Hfshiz9MZW3l2uMi0cgUUd1ePxfu6Q6h'
access_secret = 'WX62pbWlO1Q1gx9fOldzyffLMwh9kgsvc7AYld9OCNMj7'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret) 
api = tweepy.API(auth)


# Kafka Configuration
producer = KafkaProducer(bootstrap_servers=['quickstart.cloudera:9092'])


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        # push data to kafka topic "twitter_feed"
        producer.send('twitter_feed',json.dumps(status._json).encode('utf-8'))
        print("tweet_pushed")
        time.sleep(5) ###to_change
        #print(status)
        
myStreamListener = MyStreamListener()        
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
# twitter hashtags to listen 
myStream.filter(track=['iphone','india']) ###to_change
