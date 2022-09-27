import json
import boto3
from decimal import Decimal

def lambda_handler(event, context):
    print(event['body'])
    candidateId = event['query']['candidateId']
    candidateResult = getCandidateResult(candidateId)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-*': '*'
            
        },
        'body': json.dumps(candidateResult, cls=DecimalEncoder)
    }

def getDynamoDbResource():
    return boto3.resource('dynamodb')
    
def getItem(table_name, item):
    dynamodb = getDynamoDbResource()
    table = dynamodb.Table(table_name)
    return table.get_item(Key=item)

def getCandidateResult(candidateId):
    item = {'candidateId' : candidateId}
    dynamodbObj = getItem('CandidateTestResults', item)
    return dynamodbObj['Item']

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)