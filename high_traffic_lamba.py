"""
Lambda to set up SNS email notification, if number of views exceeds 1000 for top 10 items.
"""

from __future__ import print_function
import boto3
import base64
import json

# here is through boto3 client, thougt it could be made just via setting up aa SNS destination!
client = boto3.client('sns')
topic_arn = 'arn:aws:sns:us-east-2:225351114962:traffic_overhelm'


def lambda_handler(event, context, config_args=None):
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]

    views_sum = 0
    for rec_to_check in deserialized_data[:9]:
        views_sum += int(rec_to_check["views_count"])

    if views_sum > 1000:
        try:
            client.publish(TopicArn=topic_arn, Message='Hight traffic to top 10!!!', Subject='Hight traffic Alarm')
            print('Successfully delivered alarm message')
        except Exception as e:
            print('Delivery failure:' + str(e))
