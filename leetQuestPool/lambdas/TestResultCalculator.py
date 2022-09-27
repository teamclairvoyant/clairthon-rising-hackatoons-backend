import json
import boto3
import uuid


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
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-*': '*'
            
        },
        'body': json.dumps(testScore)
    }

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
            result['correct'] = True
            score = score + 1
        else:
            result['correct'] = False
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