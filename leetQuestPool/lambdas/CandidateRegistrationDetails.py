import json
import boto3
import uuid

def lambda_handler(event, context):
    table_name = 'CandidateRegistrationDetails'
    item = event['body']
    id = item.get('id', None)
    if id == None:
        id = uuid.uuid1().hex
        item['id'] = id
    insertItem(table_name, item)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-*': '*',
        },
        'body': json.dumps(id)
    }

def getResource():
    return boto3.resource('dynamodb')

def insertItem(table_name, item):
    dynamodb = getResource()
    table = dynamodb.Table(table_name)
    table.put_item(Item=item)
    return 200
    