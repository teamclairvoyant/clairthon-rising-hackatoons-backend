import json
import boto3
import uuid
import requests

url = 'https://hooks.slack.com/services/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'

def lambda_handler(event, context):
    body = event['body']
    testDetails = body['testQuestions']
    generatedTestData = getGeneratedTestData('CandidateTestLinkDetails', body['candidateDetailsId'])
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
    body['testTaken']=True
    body['resultId']=id
    persistCandidateTestDetails('CandidateTestResults', testScore)
    persistCandidateTestDetails('CandidateTestLinkDetails', body)
    publishToSlack(body['name'], body['email'], testScore)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-*': '*',
            'Content-type': 'application/json'
        },
        'body': json.dumps(testScore)
    }

def publishToSlack(name, email, testScore):
    data = {
        'candidateId':testScore['candidateId'],
        'name':name,
        'email': email,
        'totalScore':str(testScore['totalScore']) +'/'+ str(testScore['totalQuestions']),
        'testStatus':testScore['testStatus']
    }
    payload = {'text': json.dumps(data)}
    header = {
        'Access-Control-Allow-*': '*',
        'Content-type': 'application/json'
    }
    body = requests.post(url, json = payload, headers = header)


def getGeneratedTestData(tableName, candidateId):
    item = {'candidateDetailsId' : candidateId}
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