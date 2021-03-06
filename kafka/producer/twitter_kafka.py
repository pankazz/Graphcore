from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''


# Kafka settings
topic = 'twitterstream'
# setting up Kafka producer
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)


#This is a basic listener that just put received tweets to kafka cluster.
class StdOutListener(StreamListener):
    def on_data(self, data):
	
        producer.send_messages(topic, data.encode('utf-8'))
        #print data
        return True

    def on_error(self, status):
        print("nahi aa raha" + str(status))



if __name__ == '__main__':
    print 'running the twitter-stream python code'
    
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    
    while True:
        try:
            stream.sample()
           # stream.filter(languages=["en"],track = ["microsoft"])
        except:
            pass
