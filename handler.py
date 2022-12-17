import boto3
import face_recognition
import pickle
import os
import csv
import json
import shutil
from boto3.dynamodb.conditions import Key

# Function to read the 'encoding' file
def open_encoding(filename):
	file = open(filename, "rb")
	data = pickle.load(file)
	file.close()
	return data

def face_recognition_handler(event, context):	
	s3 = boto3.client('s3')

	bucket = event['Records'][0]['s3']['bucket']['name']
	key = event['Records'][0]['s3']['object']['key']
	video_file = s3.get_object(Bucket=bucket, Key=key)
	s3.download_file(bucket, key, '/tmp/'+key)
	print(os.path.isfile('/tmp/' + key))

	key_name_only = os.path.splitext(key)[0]
	video_file_path = "/tmp/"+key
	folder_path = "/tmp/images/"+key_name_only

	os.makedirs(folder_path, exist_ok=True)
	os.system("ffmpeg -i " + str(video_file_path) + " -r 1 " + str(folder_path+"/") + "image-%3d.jpeg")

	provided_file = open_encoding("encoding")
	names = provided_file['name']
	provided_encodings = provided_file['encoding']
	ans = ""

	for filename in os.scandir(folder_path):
		if filename.is_file():
			image_path = filename.path
			img_file = face_recognition.load_image_file(image_path)
			face_encodings = face_recognition.face_encodings(img_file)
			for face_encoding in face_encodings:
				results = face_recognition.compare_faces(provided_encodings, face_encoding)
				i = 0
				for result in results:
					if result:
						break
					else:
						i = i + 1
				size = len(results)
				if i < size:
					ans = names[i]
					break
			if ans == "":
				continue
			else:
				break
	
	if ans == "":
		ans = "No Match"
	print(ans)
	
	dynamodb = boto3.resource('dynamodb')
	table = dynamodb.Table('StudentData2')
	response = table.query(KeyConditionExpression=Key('name').eq(ans))
	print(response)
	
	student_name=""
	student_major = "" 
	student_year = ""
	
	for item in response['Items']:
		student_name = item['name']
		student_major = item['major']
		student_year = item['year']
	
	csv_filename = '/tmp/'+key_name_only+'.csv'
	
	with open(csv_filename, 'w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(["name", "major", "year"])
		writer.writerow([student_name, student_major, student_year])
		
	output_bucket = "output-student-info"
	
	with open(csv_filename, "rb") as f:
		s3.upload_fileobj(f, output_bucket, key_name_only+'.csv')

	shutil.rmtree(folder_path)
	
	return {
        'message' : 'Done'
    }
