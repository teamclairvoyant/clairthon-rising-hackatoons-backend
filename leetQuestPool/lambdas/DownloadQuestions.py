import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    listOfListQuestion=[] 
    secondTemp = 'question answer\n'
    path = '{skill}/{levelOfDifficulty}/theory/'
    body = json.loads(event['body'])
    skill = body['skill']
    levelOfDifficulty = body['levelOfDifficulty']
    numberOfQuestions = body['noOfQuestions']
    
    s3List = getFilesFromS3('leetquestpool', path.format(skill=skill, levelOfDifficulty=levelOfDifficulty))
    maxQuestions = len(s3List) if len(s3List) < numberOfQuestions else numberOfQuestions
    for obj in s3List[:maxQuestions]:
        data = getS3FileContent('leetquestpool', obj)
        listOfQuestion = []
        for row in data.values():
            listOfQuestion.append(row)
        listOfListQuestion.append(listOfQuestion)
            
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'text/csv',
            'Content-disposition': 'attachment; filename=questionBank.csv'
        },
        'body': json.dumps(listOfListQuestion)
    }

def getFilesFromS3(bucketName, startAfter):
    try :
        theObj = s3.list_objects_v2(Bucket=bucketName, StartAfter=startAfter)
        theObjContents = theObj['Contents']
        finalContent = []
        for obj in theObjContents:
            if obj['Key'].startswith(startAfter):
                finalContent.append(obj['Key'])
        return finalContent
    except Exception as e:
        print(e)
        return []

def getS3FileContent(bucketName, key):
    data = s3.get_object(Bucket=bucketName, Key=key)
    jsonData = json.loads(data['Body'].read())
    return jsonData