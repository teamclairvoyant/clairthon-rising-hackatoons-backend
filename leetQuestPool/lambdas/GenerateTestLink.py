import json
import uuid
import boto3
import math

s3 = boto3.client('s3')


def lambda_handler(event, context):
    candidateId = event['query']['candidateId']
    candidateDetails = getRegisteredCandidate(candidateId)
    technologyWiseDistribution = getQuestionPercentageDistribution(candidateDetails)
    testQuestions = getTestQuestions(technologyWiseDistribution)
    print(testQuestions)
    candidateDetails['testQuestions'] = testQuestions
    candidateDetails['id']=uuid.uuid1().hex
    persistCandidateTestDetails('CandidateTestLinkDetails', candidateDetails)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-*': '*'
            
        },
        'body': json.dumps(candidateDetails)
        
    }

def getRegisteredCandidate(candidateId):
    # Get candidate details
    item = {'id' : candidateId}
    dynamodbObj = getItem('CandidateRegistrationDetails', item)
    candidateDetails = getCandidateDetails(dynamodbObj['Item'])
    return candidateDetails

def getCandidateDetails(dynamodbObj):
    techSkills = [x['name'] for x in dynamodbObj['techSkills']]
    if '-' in dynamodbObj['techExperience'] : 
        exp = dynamodbObj['techExperience'].split('-')[1]
        exp = int(exp)
        exp = exp-1
    else :
        exp = dynamodbObj['techExperience']
    candidateDetails={
		'candidateDetailsId': dynamodbObj['id'],
		'openPosition': dynamodbObj['openPosition'],
		'technology': techSkills,
		'techExperience': exp,
		'testTaken': False
        
    }
    return candidateDetails

def getQuestionPercentageDistribution(candidateDetails):
	exp = candidateDetails['techExperience']
	technicalSkills = candidateDetails['technology']
	position = candidateDetails['openPosition']

	questionDistribution = {}
	technologyWiseDistribution = []
	numOfSkills = len(technicalSkills)

	if exp > 5 :
		questionDistribution['hard'] = 50
		questionDistribution['medium'] = 30
		questionDistribution['easy'] = 20
		
	elif exp >= 3 and exp < 5:
		questionDistribution['hard'] = 30
		questionDistribution['medium'] = 40
		questionDistribution['easy'] = 30
		
	else:
		questionDistribution['hard'] = 10
		questionDistribution['medium'] = 40
		questionDistribution['easy'] = 50
		
	for skill in technicalSkills:
		temp = {
			'skill': skill,
			'techPercentage': math.floor(100/numOfSkills),
			'questionDistribution': questionDistribution
		}
		technologyWiseDistribution.append(temp)
	return technologyWiseDistribution

def getTestQuestions(technologyWiseDistribution):
    testQuestions = {}
    testQuestions = populateTestQuestions(testQuestions, technologyWiseDistribution)
    return testQuestions

def populateTestQuestions(testQuestions, technologyWiseDistribution):
    totalQuestions = 4
    startAfter = '{skill}/{levelOfDifficulty}/mcq/'
    allSkillKeys = []
    usedKeys = []
    countOfQuestions = 0
    for techDist in technologyWiseDistribution:
        skill = techDist['skill'].lower()
        techPercentage = techDist['techPercentage']
        questionDistribution = techDist['questionDistribution']
        numberOfQuestionForSkill = math.ceil(totalQuestions *  techPercentage/100)
        testQuestions[skill] = []
        for key, val in questionDistribution.items():
            mcqObj = getFilesFromS3('leetquestpool', startAfter.format(skill=skill, levelOfDifficulty=key))
            if len(mcqObj) != 0:
                allSkillKeys.extend([x for x in mcqObj if x not in allSkillKeys])
                numberOfQuestionsPerLevel = math.ceil(numberOfQuestionForSkill * val/100)
                numberOfQuestionsInS3 = len(mcqObj)
                rangeVal = numberOfQuestionsPerLevel if numberOfQuestionsPerLevel < numberOfQuestionsInS3 else numberOfQuestionsInS3
                i = 0
                for obj in mcqObj:
                    if i < rangeVal and obj not in usedKeys:
                        usedKeys.append(obj)
                        data = getS3FileContent('leetquestpool', obj)
                        testQuestions[skill].append(data)
                        countOfQuestions = countOfQuestions+1
                        i = i+1
                    else :
                        break
    
    remainingKeys = [x for x in allSkillKeys if x not in usedKeys]
    remainingNumberOfQues = totalQuestions - countOfQuestions
    if len(remainingKeys) > remainingNumberOfQues:
        remainingKeys = remainingKeys[:remainingNumberOfQues]
    
    for key in remainingKeys:
        if key not in usedKeys:
            usedKeys.append(key)
            data = getS3FileContent('leetquestpool', key)
            testQuestions[skill].append(data)
    return testQuestions

def getFilesFromS3(bucketName, startAfter):
    try :
        theObj = s3.list_objects(Bucket=bucketName)
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

def getDynamoDbResource():
    return boto3.resource('dynamodb')

def persistCandidateTestDetails(tableName, item):
	dynamodb = getDynamoDbResource()
	table = dynamodb.Table(tableName)
	return table.put_item(Item=item)

def getItem(table_name, item):
    dynamodb = getDynamoDbResource()
    table = dynamodb.Table(table_name)
    return table.get_item(Key=item)