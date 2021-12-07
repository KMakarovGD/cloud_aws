"""
checks the dynamoDB TABLE WITH fraudulent IP'S
https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html

https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html  - вот с кинесисом интеграция
"""

import json
import boto3
import base64

from boto3.dynamodb.conditions import Key
def lambda_handler(event, context):
    """
    filters wrong ip's
    :return: virified Views/Reviews
    """
    # в каком виде данные приходят в лямбду?!

    result_records = []

    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('wrong_ip')

    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]

    #many to many data tranfer
    for rec_to_check in deserialized_data:
        user_ip =rec_to_check["user_ip"]

        suspicios_ip = table.query(
            KeyConditionExpression=Key('ip').eq(user_ip)
        )

        if suspicios_ip["Count"] == 0:
            result_records.append(rec_to_check)

    #yes, exactly thiss way AWS will append all extradata
    return result_records

# Returning a value
#
# Optionally, a handler can return a value. What happens to the returned value depends
# on the invocation type and the service that invoked the function. For example:
#
#     If you use the RequestResponse invocation type, such as Synchronous invocation,
# AWS Lambda returns the result of the Python function call to the client invoking the Lambda function
#  (in the HTTP response to the invocation request, serialized into JSON). For example, AWS Lambda console
#  uses the RequestResponse invocation type, so when you invoke the function on the console, the console will display the returned value.
#
#     If the handler returns objects that can't be serialized by json.dumps, the runtime returns an error.
#
#     If the handler returns None, as Python functions without a return statement implicitly do, the runtime returns null.
#
#     If you use an Event an Asynchronous invocation invocation type, the value is discarded.

