import json
import urllib.parse
import boto3
import pandas as pd
import uuid
import os

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'], index_col=0)

        startProcess(df)
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def startProcess(df):
	listToSave = []
	for _, row in df.iterrows():
		technology = row['technology']
		typeOfQuestion = row['typeOfQuestion']
		levelOfDifficulty = row['levelOfDifficulty']
		question = row['question']
		isMcq = row['isMcq']
		mcqOptions = row['mcqOptions']
		answer = row['answer']

		if isMcq:
			path = (technology+'/'+levelOfDifficulty+'/'+'mcq/')
			mcqOption_list = mcqOptions.split(',')

			item = {
				'question': question,
				'option 1':mcqOption_list[0],
				'option 2':mcqOption_list[1],
				'option 3':mcqOption_list[2],
				'option 4':mcqOption_list[3],
				'answer': answer
			}
		else:
			path = (technology+'/'+levelOfDifficulty+'/'+'theory/')
			item = {
				'question': question,
				'answer': answer
			}
		saveToFile(item, path, listToSave)
	success = addToS3(listToSave)
	purgeData(listToSave)
	
# save files to a list to upload to s3
def saveToFile(item, path, listToSave):
	fileName = uuid.uuid1().hex
	listToSave.append(path+fileName)
	with open('/tmp/'+fileName, 'w') as file:
		file.write(json.dumps(item))

# upload to s3 from list of files
def addToS3(listToSave):
	print('adding to s3')
	for file in listToSave:
		fileName = file.split('/')[-1]
		with open('/tmp/'+fileName, "rb") as f:
			s3.upload_fileobj(f, "leetquestpool", file)
		# s3.upload_fileobj('/tmp/'+fileName, 'leetquestpool', file)
		# s3.Bucket('leetquestpool').upload_file('/tmp/'+fileName,file)
	return True

# purge data from tmp location after successful upload
def purgeData(listToSave):
    for file in listToSave:
    	fileName = file.split('/')[-1]
    	tmp_file_path = '/tmp/'+fileName
    	if os.path.exists(tmp_file_path):
    		os.remove(tmp_file_path)