from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
tweets = spark.read.json(#path to s3 file)
tweets.registerTempTable("tweets")
ms_tweets = spark.sql("select  retweeted_status['user']['screen_name'] , user['screen_name'] from tweets where (lower(text) like '%microsoft%') and retweeted_status is not Null ")
v1 = ms_tweets.rdd.map(lambda x:(x[0],x[1])).distinct()
v2 = ms_tweets.rdd.map(lambda x:(x[1],x[0])).distinct()
sc =SparkContext.getOrCreate()
combo = sc.union([v1,v2])
combo = combo.filter(lambda x:x[0] != x[1])
nodes = combo.map(lambda x:(x[0],1))
deg_agg = nodes.reduceByKey(lambda accum, n: accum + n)
k_d = deg_agg.filter(lambda x:x[1]<=1).keys().collect()
new_rdd = combo.filter(lambda x: x[0] not in k_d and x[1] not in k_d)
while len(k_d) > 0:
    new_rdd.persist()
    print new_rdd.count()
    #b = new_rdd.map(lambda x:(x[0],x[1]))
    #c = new_rdd.map(lambda x:(x[1],x[0]))
    
    #combined = b.toDF().unionAll(c.toDF())
    node = new_rdd.map(lambda x:(x[0],1))
    
    
    
    tots = node.reduceByKey(lambda accum, n: accum + n)
    
    
    
    
    
    k_d_2 = tots.filter(lambda x:x[1]<=1)
    
    if k_d_2.isEmpty() == True:
        print("break")
        break
    else:
        k_d = k_d_2.keys().collect()
    
    print len(k_d)
    
    new_rdd = new_rdd.filter(lambda x: x[0] not in k_d and x[1] not in k_d)
    new_rdd.persist()


new_rdd.toDF().toPandas().to_csv('graph3.csv',index=False)
