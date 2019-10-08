#!/usr/bin/env python3

import os
import boto3
from botocore.exceptions import NoCredentialsError

dir_path = os.path.dirname(os.path.realpath(__file__))
#https://medium.com/@devopslearning/100-days-of-devops-day-70-introduction-to-boto3-98a257749dd0
def list_buckets():
	s3 = boto3.resource('s3')
	for bucket in s3.buckets.all():
		print(bucket.name)

# Code from https://medium.com/bilesanmiahmad/how-to-upload-a-file-to-amazon-s3-in-python-68757a1867c6
def upload_to_aws(local_file, bucket, s3_file):
	s3 = boto3.client('s3')
	try:
		s3.upload_file(local_file, bucket, s3_file)
		print("Upload Successful")
		return True
	except FileNotFoundError:
		print("The file was not found")
		return False
	except NoCredentialsError:
		print("Credentials not available")
		return False

list_buckets
uploaded = upload_to_aws('{}/test.csv'.format(dir_path), 'potzenhotz-python-test', 'test.csv')
print(uploaded)




