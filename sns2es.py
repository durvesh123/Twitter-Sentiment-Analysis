from __future__ import print_function
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from certificate.my_AWS_key import *

import json

# ES Client
REGION = "us-east-1"
host = 'search-tweet-kkj4pr2lzt2sfl3jzxq2a45a7e.us-east-1.es.amazonaws.com'
INDEX = 1
awsauth = AWS4Auth(ACCESS_KEY, SECRET_KEY, REGION, 'es')
es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def indextoES(event, context):
    msg = event['Records'][0]['Sns']['Message']
    print("From SNS: " + msg)
    message = {
        "text": event['Records'][0]['Sns']['Message']['text'],
        "lat": event['Records'][0]['Sns']['Message']['lat'],
        "lng": event['Records'][0]['Sns']['Message']['lng'],
        "sentiment": event['Records'][0]['Sns']['Message']['sentiment']
    }
    es.index(index=INDEX, body=message, doc_type="tweet")
    return message
