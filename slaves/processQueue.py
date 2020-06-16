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
import logging


LOG_FILE = 'processQueue.log'
PATH_DARKNET = "/home/ubuntu/darknet"
PATH_PROJ = "/home/ubuntu/CloudComputingProj1"
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/056594258736/video-process'
OUTPUT_FILENAME = "results.txt"
VIDEO_BUCKET_NAME = "worm4047bucket1"
RESULT_BUCKET_NAME = "worm4047bucket2"

logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

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
    return boto3.client(type, region_name=REGION)
    # return boto3.client(type,aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,aws_session_token=SESSION_TOKEN,region_name=REGION)

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

'''

S3 FUNCTIONS 

'''

def upload_file(file_name, object_name=None):
    s3_client = get_client('s3')
    max_retries = 5
    while max_retries > 0:
        try:
            response = s3_client.upload_file(file_name, RESULT_BUCKET_NAME, object_name)
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


def download_file(OBJECT_NAME, FILE_NAME):
    print("Downloading File")
    logging.info("Downloading File ")
    s3_client = get_client('s3')
    s3_client.download_file(VIDEO_BUCKET_NAME, OBJECT_NAME, FILE_NAME)

'''

SQS FUNCTIONS 

'''

def delete_msg(message):
    print("Successfully processed & deleting  Messages ")
    logging.info('Successfully processed & deleting  Messages ')
    sqs_client = get_client('sqs')
    while True:
        try:
            sqs_client.delete_message(QueueUrl=QUEUE_URL,ReceiptHandle=message['ReceiptHandle'])
            break
        except Exception as e:
            print(e)
        time.sleep(0.5)


def handle_visibility(reciept_handle, value):
    sqs_client = get_client('sqs')
    logging.info("Handing visibility ")
    try:
        response = sqs_client.change_message_visibility(
            QueueUrl= QUEUE_URL,
            ReceiptHandle=reciept_handle,
            VisibilityTimeout=value
        )
        print(response)
    except Exception as e:
        print(e)


'''

PROCESS MESSAGE FUNCTION 

'''

def processMessage(li):
    print("Processing Messages ", len(li))
    start_time = time.time()
    results = ""
    
    for message in li:
        logging.info(message)
        object_name, _ = message['Body'].split(':')
        # temp_file_name = object_name + '.h264'
        temp_file_name = object_name

        # Download File
        max_retries = 5
        while max_retries > 0:
            try:
                download_file(object_name, temp_file_name)
                break
            except Exception as e:
                logging.error(e)
            max_retries -= 1
        
        # Couldn't Download File
        if max_retries == 0:
            # Put message back in queue
            handle_visibility(message['ReceiptHandle'], 0)
            return False

        try:
            command = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights " + temp_file_name + " > " + OUTPUT_FILENAME 
            # command="ping google.com"
            print("Command executing")

            logging.info("Darknet started " + command )
            
            process = subprocess.Popen(command, shell=True)
            process.wait()
            os.chdir(PATH_DARKNET)
            logging.info("Darknet Finished")
            object_list = get_objects(OUTPUT_FILENAME)
            if len(object_list) == 0:
                results = "no object detected"
            else:
                results = ", ".join(object_list)
            # results[object_name] = object_list
            
            
            if(upload_results(object_name, results)):
                print("Results Uploaded")
                print("--- %s seconds ---" % (time.time() - start_time))
                logging.info("Results Uploaded")
                delete_msg(message)
                logging.info("--- %s seconds ---" % (time.time() - start_time))
                return True
            else:
                print("Result Upload Failed")
                logging.info("Result Upload Failed")
                handle_visibility(message['ReceiptHandle'], 0)
                return False

        except Exception as e:
            handle_visibility(message['ReceiptHandle'], 0)
            print(e)
            print("Message Processing Failed")
            logging.info("Message Processing Failed")
            logging.error(e)
            return False

            


if __name__ == '__main__':

    # To clear log file
    open(LOG_FILE, 'w').close()
    # Get Creds From Cred File
    # Only needed for local testing
    ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, REGION = "", "", "", ""
    get_creds()

    os.chdir(PATH_DARKNET)

    # Get client
    sqs_client = get_client('sqs')
    first_time = True

    # Count processed
    count = 0

    while True:
        # Check if first_time i.e executed by master
        if first_time:
            li = [{'Body':sys.argv[1], 'ReceiptHandle':sys.argv[2]}]
            res = processMessage(li)
            # time.sleep(300)
            if res:
                count += 1
            else:
                print("\n\n UNABLE TO PROCESS \n\n")
                break
            first_time = False
        else:
            # Get messages again
            li = sqs_client.receive_message(QueueUrl=QUEUE_URL, VisibilityTimeout=600)
            if "Messages" not in li or li['Messages'] == 0:
                # No more messages to process so break out of loop
                print("No More messages to process")
                break
            else:
                res = processMessage(li['Messages'])
                if res:
                    count += 1
                else:
                    print("\n\n UNABLE TO PROCESS \n\n")
                    break
                # time.sleep(300)
    print(" \n\n :::::::::: Number of messages processed  ::::::::::::::: \n", count)
    print("\n\n")
    os.chdir(PATH_PROJ)

            
 

