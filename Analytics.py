#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 04:23:05 2019

@author: cloudera
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext

sc = SparkContext(appName="PysparkNotebook")

sqlContext = HiveContext(sc)

#report 1
userByLocation = sqlContext.sql("select count(*),location from twitter_user group by location")
userByLocation.write.mode('overwrite').saveAsTable("default.report_user_by_location")

#report 2
top3MaxFollowerByLocation = sqlContext.sql("""select * from ( select name, screen_name, followers_count, 
    rank() over ( partition by location order by location desc) as rank from twitter_user ) t
    where rank < 4""")
top3MaxFollowerByLocation.write.mode('overwrite').saveAsTable("default.report_max_follow_by_location")

#report 3
highestStatusesByUser = sqlContext.sql("""select * from ( select name, screen_name, statuses_count, 
    rank() over ( partition by location order by location desc) as rank from twitter_user ) t
    where rank <= 10""")
highestStatusesByUser.write.mode('overwrite').saveAsTable("default.highestStatusesByUser")

#report 4
highestFriendsList = sqlContext.sql("""select * from ( select name, screen_name, friends_count, 
    rank() over ( partition by location order by location desc) as rank from twitter_user ) t
    where rank <= 10""")
highestFriendsList.write.mode('overwrite').saveAsTable("default.highestFriendsList")

sc.stop()
