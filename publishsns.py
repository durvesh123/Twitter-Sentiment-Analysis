import imp
import sys
sys.modules["sqlite"] = imp.new_module("sqlite")
sys.modules["sqlite3.dbapi2"] = imp.new_module("sqlite.dbapi2")
import nltk
import boto3
from certificate.my_AWS_key import *
import json
from textblob import TextBlob
import ast

# Create SQS client
sqs = boto3.resource('sqs', region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
queue = sqs.get_queue_by_name(QueueName='tweets.fifo')

# Create SNS client
sns = boto3.client('sns',region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)



def publishToSNS(event,context):
    # Process messages by printing out body
    while True:
        for message in queue.receive_messages(AttributeNames=['All'],MessageAttributeNames=['All',],VisibilityTimeout=1,WaitTimeSeconds=0):
            # Print out the body of the message
            sqsdata={}
            sqsdata=eval(message.body)

            snspublish={
                "text":sqsdata['text'],
                "lat": sqsdata['lat'],
                "lng": sqsdata['lng'],
                "sentiment": sentiment_analysis(sqsdata['text'])
            }
            response = sns.publish(
                #Message=str(snspublish)
                TopicArn='arn:aws:sns:us-east-1:274213482424:tweets',
                Message=json.dumps({'default': json.dumps(snspublish)}),
                MessageStructure='json'
            )
           
            message.delete()


def sentiment_analysis(str):
    blob=TextBlob(str)
    sentiment_polarity=blob.polarity
    if sentiment_polarity >= 0.1:
        sentiment_polarity = 'positive'
    elif sentiment_polarity <= -0.1:
        sentiment_polarity = 'negative'
    else:
        sentiment_polarity= 'neutral'
    return sentiment_polarity

