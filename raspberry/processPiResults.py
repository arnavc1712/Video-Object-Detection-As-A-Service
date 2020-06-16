import boto3
from botocore.exceptions import ClientError
import logging
import subprocess
import string
import random
import time
import os
import sys
import time
import json
from ProgressPercentage import *
import logging



def upload_file(file_name, object_name=None):
    RESULT_BUCKET_NAME = "worm4047bucket2"
    s3_client = get_client('s3')
    max_retries = 5
    while max_retries > 0:
        try:
            response = s3_client.upload_file(file_name, RESULT_BUCKET_NAME, object_name, Callback=ProgressPercentage(file_name))
            break
        except ClientError as e:
            logging.error(e)
        max_retries -= 1
    return max_retries > 0

def upload_results(object_name, results):
    file_name = object_name
    with open(file_name, 'w+') as f:
        f.write(results)
    return upload_file(file_name, object_name)


def get_creds():
    global ACCESS_KEY
    global SECRET_KEY
    global SESSION_TOKEN
    global REGION
    cred_file = "cred.json"
    with open(cred_file) as f:
        data = json.load(f)
        ACCESS_KEY = data['aws_access_key_id']
        SECRET_KEY = data['aws_secret_access_key']
        SESSION_TOKEN = data['aws_session_token']
        REGION = data['region']

def get_client(type):
    global ACCESS_KEY
    global SECRET_KEY
    global SESSION_TOKEN
    global REGION
    # return boto3.client(type, region_name=REGION)
    return boto3.client(type,aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,aws_session_token=SESSION_TOKEN,region_name=REGION)


def get_objects(FILENAME):
    logging.info(os.getcwd())
    result = dict()
    object_set = set()
    try:
        f = open(FILENAME, 'r')
        temp_data = f.read().split('\n')
        data = dict()
        currfps = 0
        obj_in_frame = []
        for lines in temp_data:
            lines = lines.replace('\n', "")
            if 'FPS' in lines:
                if currfps > 0 and len(obj_in_frame) > 0:
                    data[currfps] = (obj_in_frame)
                    obj_in_frame = []
                currfps += 1
            elif '%' in lines:
                obj_in_frame.append(lines)
        

        for key in data:
            object_map = []
            for obj in data[key]:
                obj_name, obj_conf = obj.split()
                
                obj_name = (obj_name.replace(':',''))
                object_set.add(obj_name)
                obj_conf = (int)(obj_conf.replace('%',''))
                object_map.append({obj_name:(obj_conf*1.0)/100})
            result[key] = (object_map)
    except Exception as e:
        pass
    # return {'results' : [result]}
    return list(object_set)

if __name__ == '__main__':
    ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, REGION = "", "", "", ""
    OUTPUT_FILENAME = "results.txt"
    PATH_DARKNET = "/home/pi/darknet/"
    get_creds()
    object_list = get_objects(PATH_DARKNET + OUTPUT_FILENAME)
    object_name = sys.argv[1]
    results = ""
    if len(object_list) == 0:
        results = "no object detected"
    else:
        results = ", ".join(object_list)
    # results[sys.argv[1]] = object_list
    upload_results(object_name, results)