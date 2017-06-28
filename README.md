# Graphcore
Graphcore is my project at Data Engineering Fellowship program at Insight Data Science, New York. In this project I have implemented k-core for a graph in a distributed system. I streamed twitter data for 2 weeks collecting ~25 million tweets and extracted the tweets that have been retweeted at least once. I filtered the tweets based on keywords which in my case are named of top tech companies like Google, Microsoft and built the retweet network for each keyword. By implementing k-core I pruned the graph by 1 degree until I reached the core of a graph giving the list of most rewteeted users or users who retweeted the tweets mentioning keywords the most. I used batch processing in spark for the above mentioned tasks. 

# Motivation
The motivation behind this project is to explore the most connected users in a dense large graph with number of edges ranging from few hundred thousands to millions. The project can be used by marketing agencies, brand managers and consultants or public figures to study the information diffusion on social media and identify the central users from which it's originating. 

# Pipeline
For this project I used the below pipeline 
![pipeline](https://github.com/pankazz/Graphcore/blob/master/images/pipeline.PNG)

# Demo
The demo is available at pankajchhabra.site which shows by default 4-core graph of retweet network of tweets containing the keyword 'Microsoft'. By appending /4c to the url graph to access 3-core graph
