import json
import boto3
import uuid
import requests
import urllib.parse
import os

url = os.environ['url']
feedbackUrl = os.environ['feedbackUrl']
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    body = getS3Content(bucket, key)
    candidateDetailsId = body['candidateDetailsId']
    
    testDetails = body['testQuestions']
    generatedTestDataItem = {'candidateDetailsId' : candidateDetailsId}
    generatedTestData = getTableData('CandidateTestLinkDetails', generatedTestDataItem)
    candidateDataItem = {'id' : candidateDetailsId}
    candidateData = getTableData('CandidateRegistrationDetails', candidateDataItem)
    testScore = {}
    totalQuestions = 0;
    totalScore = 0;
    techwiseResult = []
    
    totalQuestions = totalQuestions + len(testDetails)
    testResponses, score = getTestScore(testDetails, generatedTestData['testQuestions'])
    techwiseResult.extend(testResponses)
    totalScore = totalScore + score
    
    id = uuid.uuid1().hex
    testScore['candidateId']=body['candidateDetailsId']
    testScore['id'] = id
    testScore['totalScore'] = totalScore
    testScore['totalQuestions'] = totalQuestions
    testScore['testResult'] = techwiseResult
    testScore['testStatus'] = getTestStatus(totalScore, totalQuestions)
    testScore['feedbackStatus'] = feedbackUrl+candidateDetailsId
    
    candidateData['result'] = testScore['testStatus']
    
    body['testTaken']=True
    body['resultId']=id
    
    persistCandidateTestDetails('CandidateTestResults', testScore)
    persistCandidateTestDetails('CandidateTestLinkDetails', body)
    persistCandidateTestDetails('CandidateRegistrationDetails', candidateData)
    
    publishToSlack(body['name'], body['email'], testScore)


def publishToSlack(name, email, testScore):
    data = {
        'candidateId':testScore['candidateId'],
        'name':name,
        'email': email,
        'totalScore':str(testScore['totalScore']) +'/'+ str(testScore['totalQuestions']),
        'testStatus':testScore['testStatus'],
        'feedbackStatus':testScore['feedbackStatus']
    }
    payload = {'text': json.dumps(data)}
    header = {
        'Access-Control-Allow-*': '*',
        'Content-type': 'application/json'
    }
    body = requests.post(url, json = payload, headers = header)


def getTableData(tableName, item):
    dynamodb = getDynamoDbResource()
    table = dynamodb.Table(tableName)
    data = table.get_item(Key=item)
    return data['Item']
    

def getDynamoDbResource():
    return boto3.resource('dynamodb')

def persistCandidateTestDetails(tableName, item):
    dynamodb = getDynamoDbResource()
    table = dynamodb.Table(tableName)
    return table.put_item(Item=item)

def getTestScore(testResults, generatedTestData):
    score = 0
    for result in testResults:
        answer = getAnswer(result['questionId'], generatedTestData)
        if (answer == result['candidateAnswer']):
            result['correctness'] = "correct"
            score = score + 1
        else:
            result['correctness'] = "incorrect"
    return (testResults, score)

def getAnswer(id, generatedTestData):
    for entry in generatedTestData:
        if entry['questionId'] == id:
            print(entry['questionId'])
            return entry['answer']

def getTestStatus(totalScore, totalQuestions):
    percent = totalScore/totalQuestions * 100
    if percent > 50 :
        return 'Pass'
    else:
        return 'Fail'

def getS3Content(bucket, key):
    data = s3.get_object(Bucket=bucket, Key=key)
    jsonData = json.loads(data['Body'].read())
    return jsonData