import json
import boto3
from decimal import Decimal

TABLE = 'CandidateRegistrationDetails'

def lambda_handler(event, context):

    query = event['query']
    candidateId = query.get('candidateId', None)
    if candidateId != None:
        response = getCandidate(candidateId)
    else:
        response = getAllCandidates()
            
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-*': '*'
        },
        'body': json.dumps(response, cls=DecimalEncoder)
    }

def getCandidate(candidateId):
    item = {
        'id': candidateId
    }
    candidateDetail = getItem(item)
    return candidateDetail['Item']

def getItem(item):
    dynamodb = getDynamoDbResource()
    table = dynamodb.Table(TABLE)
    return table.get_item(Key=item)

def getDynamoDbResource():
    return boto3.resource('dynamodb')

def getAllCandidates():
    dynamodb = getDynamoDbResource()
    table = dynamodb.Table(TABLE)
    response = table.scan()
    return response['Items']

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)



