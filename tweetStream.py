import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from certificate.my_Twitter_key import *
import boto3
from certificate.my_AWS_key import *
import random

# Create SQS client
sqs = boto3.resource('sqs', region_name='us-east-1', aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)
queue = sqs.get_queue_by_name(QueueName='tweets.fifo')

class MyStreamListener(StreamListener):
    """
    A listener handles tweets that are received from the stream.
    """
    def on_data(self,data):
        meta = json.loads(data)
        print(meta)
        try:
            if str(meta['geo']).__eq__('None'):
                lat=str(str(meta['bounding_box']['coordinates'][0][0]))
                lng=str(str(meta['bounding_box']['coordinates'][0][1]))
            else:
                lat=meta['coordinates'][0]
                lng=meta['coordinates'][1]
        except:
                lat=str(random.uniform(-120,120))
                lng=str(random.uniform(-120,120))
        
        message={
                        "text":meta['text'],
                        "lat":lat,
                        "lng":lng,
                        "id":meta['user']['id']
                }        


        if str(meta['user']['lang']).__eq__('en'):

            try:
                    response = queue.send_message(
                        MessageAttributes={
                            'text': {
                                'DataType': 'String',
                                'StringValue': str(meta['text'])
                            },
                            'id': {
                                'DataType': 'String',
                                'StringValue': str(meta['user']['id'])
                            },
                            'location': {
                                'DataType': 'String',
                                'StringValue': str(meta['user']['location'])
                            },
                            'lat': {
                                'DataType': 'String',
                                'StringValue': str(lat),
                            },
                            'lng': {
                                'DataType': 'String',
                                'StringValue': str(lng),
                            }
                        },
                        MessageBody=(
                            str(message)
                        ),
                        MessageGroupId='Tweets'
                    )
            except Exception as e:
                print(e,meta)
            return True

    def on_error(self,status):
        print('on_error')
        print("Error Status"+status)


def streamTweets(event,context):

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    l = MyStreamListener()
    stream = Stream(auth, l)
    trackList=[]
    trackList.append(event['queryStringParameters']['topic'])
    stream.filter(track=trackList)