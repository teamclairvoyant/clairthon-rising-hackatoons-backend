import json
import boto3
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    body = event['body']
    candidateId = body['candidateDetailsId']
    saveToFile(body, candidateId)
    uploadToS3(candidateId)
    purgeData(candidateId)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-*': '*'
        },
        'body': json.dumps('Test Submission Successful')
    }

def uploadToS3(fileName):
    with open('/tmp/'+fileName, "rb") as f:
        s3.upload_fileobj(f, "candidate-test-data",fileName)
    
# save files to a list to upload to s3
def saveToFile(item, fileName):
    with open('/tmp/'+fileName, 'w') as file:
        file.write(json.dumps(item))
		
def purgeData(fileName):
    tmp_file_path = '/tmp/'+fileName
    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)