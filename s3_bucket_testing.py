#!/usr/bin/env python3

import os
import boto3
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from s3fs import S3FileSystem
from botocore.exceptions import NoCredentialsError

dir_path = os.path.dirname(os.path.realpath(__file__))
#https://medium.com/@devopslearning/100-days-of-devops-day-70-introduction-to-boto3-98a257749dd0
def list_buckets():
	s3 = boto3.resource('s3')
	for bucket in s3.buckets.all():
		print(bucket.name)

# Code from https://medium.com/bilesanmiahmad/how-to-upload-a-file-to-amazon-s3-in-python-68757a1867c6
def upload_to_s3(local_file, bucket_name, s3_file):
	s3 = boto3.client('s3')
	try:
		s3.upload_file(local_file, bucket_name, s3_file)
		print("Upload Successful")
		return True
	except FileNotFoundError:
		print("The file was not found")
		return False
	except NoCredentialsError:
		print("Credentials not available")
		return False

def download_from_s3(local_file, bucket_name, s3_file):
	s3 = boto3.client('s3')
	try:
		s3.download_file(bucket_name, s3_file, local_file)
		print("Download Successful")
		return True
	except FileNotFoundError:
		print("The file was not found")
		return False
	except NoCredentialsError:
		print("Credentials not available")
		return False

#https://stackoverflow.com/questions/37703634/how-to-import-a-text-file-on-aws-s3-into-pandas-without-writing-to-disk
def download_from_s3_csv2df(bucket_name, s3_file):
	s3 = boto3.client('s3')
	try:
		obj= s3.get_object(Bucket=bucket_name, Key=s3_file)
		df = pd.read_csv(io.BytesIO(obj['Body'].read()), sep=';')
		return df
	except:
		print("Unexpected error: {}".format(sys.exc_info()[0]))
		raise

#https://stackoverflow.com/questions/53416226/how-to-write-parquet-file-from-pandas-dataframe-in-s3-in-python
def upload_to_s3_df2parquet(df, bucket_name, s3_file, p_compression='snappy'):
	s3_url = 's3://{0}/{1}'.format(bucket_name, s3_file)
	df.to_parquet(s3_url, compression=p_compression)

def append_s3_parquet(df, s3_filesystem, s3_file):
	table = pa.Table.from_pandas(df)
	writer = pq.ParquetWriter(s3_file, table.schema, filesystem=s3_filesystem)
	writer.write_table(table=table)
	writer.close()


def copy_inside_bucket(bucket_name, src_location, trg_location):
	s3 = boto3.resource('s3')
	source = {
		'Bucket': bucket_name,
		'Key': src_location
	}
	s3.meta.client.copy(source, bucket_name, trg_location)

def copy_file_to_other_bucket(src_bucket_name, src_location, trg_bucket_name, trg_location):
	s3 = boto3.resource('s3')
	source = {
		'Bucket': src_bucket_name,
		'Key': src_location
	}
	s3.meta.client.copy(source, trg_bucket_name, trg_location)


list_buckets
#uploaded = upload_to_s3('{}/test.csv'.format(dir_path), 'potzenhotz-python-test', 'test.csv')
#print(uploaded)

#downloaded = download_from_s3('{}/test_2.csv'.format(dir_path), 'potzenhotz-python-test', 'test.csv')
#print(downloaded)
#df = download_from_s3_csv2df('potzenhotz-python-test', 'test.csv')
#print(df.head)

#print("loading parquet")
#upload_to_s3_df2parquet(df, 'potzenhotz-python-test', 'parquet/test.parquet')

#print("create df2")
#df_2 = pd.DataFrame({'feld_1': ['append'], 'feld_2':[3], 'feld_3':[1909]})
#print(df_2)
#s3_filesystem = S3FileSystem()
#print('print filesystem')
#print(s3_filesystem.ls('potzenhotz-python-test'))
#append_s3_parquet(df_2,  s3_filesystem, 'parquet/test.parquet')
#upload_to_s3_df2parquet(df_2, 'potzenhotz-python-test', 'parquet/test.parquet')


copy_inside_bucket('potzenhotz-python-test', 'test.csv', 'copied/test.csv')

