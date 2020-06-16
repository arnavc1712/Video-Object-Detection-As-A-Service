import boto3
import random
import string
import time
import json
import logging
import os
import sys
import logging
from botocore.exceptions import ClientError
from os.path import isfile, join

logging.basicConfig(filename='processQueue.log')
def generate_random_object_name(stringLength = 10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    print("Uploading ", file_name)
    if object_name == None:
        object_name = generate_random_object_name()
    object_name = file_name.split('/')[-1]
    cred_file = 'cred.json'
    ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, REGION = "", "", "", ""

    with open(cred_file) as f:
        data = json.load(f)
        ACCESS_KEY = data['aws_access_key_id']
        SECRET_KEY = data['aws_secret_access_key']
        SESSION_TOKEN = data['aws_session_token']
        REGION = data['region']

    BUCKET_NAME = "worm4047bucket1"
    s3_client = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,aws_session_token=SESSION_TOKEN,region_name=REGION)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False, {}

    addToSqs(object_name, bucket)
    return True, object_name

def addToSqs(object_name, bucket_name):
    cred_file = 'cred.json'
    ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, REGION = "", "", "", ""

    with open(cred_file) as f:
        data = json.load(f)
        ACCESS_KEY = data['aws_access_key_id']
        SECRET_KEY = data['aws_secret_access_key']
        SESSION_TOKEN = data['aws_session_token']
        REGION = data['region']

    BUCKET_NAME = "worm4047bucket1"
    sqs = boto3.client('sqs',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,aws_session_token=SESSION_TOKEN,region_name=REGION)
    
    print("Adding to sqs")
    queue = sqs.get_queue_url(QueueName='video-process')

    try:
        sqs.send_message(QueueUrl=queue['QueueUrl'], MessageBody=object_name + ':' + bucket_name)
    except Exception as e:
        logging.error(e)
        return False

    return True

if __name__ =='__main__':
    start_time = time.time()
    cred_file = 'cred.json'
    ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, REGION = "", "", "", ""

    with open(cred_file) as f:
        data = json.load(f)
        ACCESS_KEY = data['aws_access_key_id']
        SECRET_KEY = data['aws_secret_access_key']
        SESSION_TOKEN = data['aws_session_token']
        REGION = data['region']

    BUCKET_NAME = "worm4047bucket1"
    VIDEO_FILE = sys.argv[1]
    for _ in range(1):
        logging.info("Uploading to S3")
        result, obj = upload_file(VIDEO_FILE, BUCKET_NAME)

        print(result, obj)
        if(result):
            logging.info("Done ", obj)
        else:
            logging.info("Upload to S3 failed")
        logging.info("--- %s seconds ---" % (time.time() - start_time))
    
