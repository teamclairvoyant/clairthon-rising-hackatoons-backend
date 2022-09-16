import json
import boto3
import uuid

def lambda_handler(event, context):
    table_name = 'CandidateRegistrationDetails'
    item = event['body']
    id = uuid.uuid1().hex
    item['id'] = id
    print(item)
    _insert_item(table_name, item)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-*': '*',
        },
        'body': json.dumps(id)
    }

def _get_resource():
    return boto3.resource('dynamodb')

def _insert_item(table_name, item):
    dynamodb = _get_resource()
    table = dynamodb.Table(table_name)
    table.put_item(Item=item)
    return 200
    