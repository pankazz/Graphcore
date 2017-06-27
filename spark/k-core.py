from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
#reading json files from s3
tweets = spark.read.json(#path to s3 file)
tweets.registerTempTable("tweets")
ms_retweets = spark.sql("select  retweeted_status['user']['screen_name'] , user['screen_name'] from tweets where (lower(text) like '%microsoft%') and retweeted_status is not Null ")
#building rdd of edges
edges_1 = ms_retweets.rdd.map(lambda x:(x[0],x[1])).distinct()
edges_2 = ms_retweets.rdd.map(lambda x:(x[1],x[0])).distinct()
sc =SparkContext.getOrCreate()
combined_edges = sc.union([edges_1,edges_2])
#excluding self retweets
combined_edges = combined_edges.filter(lambda x:x[0] != x[1])
#mapping all nodes with value 1
nodes = combined_edges.map(lambda x:(x[0],1))
#reducing the mapped rdd to get degree of all nodes
degree_aggregate = nodes.reduceByKey(lambda accum, n: accum + n)
#filter nodes with degree 1
k_degrees = degree_aggregate.filter(lambda x:x[1]<=1).keys().collect()
transformed_rdd = combined_edges.filter(lambda x: x[0] not in k_degrees and x[1] not in k_degrees)
#repeat till the condition of mininmum k+1 degrees is attained. 
while len(k_degrees) > 0:
    transformed_rdd.persist()
    print transformed_rdd.count()
    
    node = transformed_rdd.map(lambda x:(x[0],1))
    
    
    
    tots = node.reduceByKey(lambda accum, n: accum + n)
    
    
    
    
    
    k_degrees_2 = tots.filter(lambda x:x[1]<=1)
    
    if k_degrees_2.isEmpty() == True:
        print("break")
        break
    else:
        k_degrees = k_degrees_2.keys().collect()
    
    print len(k_degrees)
    
    transformed_rdd = transformed_rdd.filter(lambda x: x[0] not in k_degrees and x[1] not in k_degrees)


#writing to graph csv file
transformed_rdd.toDF().toPandas().to_csv('graph3.csv',index=False)
