# Graphcore
Graphcore is my project at Data Engineering Fellowship program at Insight Data Science, New York. In this project I have implemented k-core for a graph in a distributed system. I streamed twitter data for 2 weeks collecting ~25 million tweets and extracted the tweets that have been retweeted at least once. I filtered the tweets based on keywords which in my case are named of top tech companies like Google, Microsoft and built the retweet network for each keyword. By implementing k-core I pruned the graph by 1 degree until I reached the core of a graph giving the list of most rewteeted users or users who retweeted the tweets mentioning keywords the most. I used batch processing in spark for the above mentioned tasks. 

#Pipeline
