#!/usr/bin/env python3

import os
import boto3
import io
import pandas as pd
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

def download_from_aws(local_file, bucket, s3_file):
	s3 = boto3.client('s3')
	try:
		s3.download_file(bucket, s3_file, local_file)
		print("Download Successful")
		return True
	except FileNotFoundError:
		print("The file was not found")
		return False
	except NoCredentialsError:
		print("Credentials not available")
		return False

#https://stackoverflow.com/questions/37703634/how-to-import-a-text-file-on-aws-s3-into-pandas-without-writing-to-disk
def load_csv2df(bucket, s3_file):
	s3 = boto3.client('s3')
	try:
		obj= s3.get_object(Bucket=bucket, Key=s3_file)
		df = pd.read_csv(io.BytesIO(obj['Body'].read()))
		return df
	except:
		print("Unexpected error: {}".format(sys.exc_info()[0]))
		raise

list_buckets
#uploaded = upload_to_aws('{}/test.csv'.format(dir_path), 'potzenhotz-python-test', 'test.csv')
#print(uploaded)

#downloaded = download_from_aws('{}/test_2.csv'.format(dir_path), 'potzenhotz-python-test', 'test.csv')
#print(downloaded)

df = load_csv2df('potzenhotz-python-test', 'test.csv')
print(df.head)

