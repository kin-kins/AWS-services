import datetime, time
import os, errno
import shutil
import boto3
import sys
import numpy as np
import cv2
from threading import Thread
import threading
from elasticsearch import Elasticsearch
import base64
import json 

my_stream_name = 'later'
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY', "")
AWS_ACCESS_SECRET_KEY = os.environ.get('AWS_ACCESS_SECRET_KEY', "")
bucket = os.environ.get('BUCKET', '')

LOCATION = os.environ.get('LOCATION', '')
LINE = os.environ.get('LINE', '')
CAMERA = os.environ.get('CAMERA', '')

INDEX_NAME = os.environ.get('INDEX_CAMERA', '')
HOST = os.environ.get('HOST', 'ip of host')
PORT = int(os.environ.get('PORT', 80))
URL = os.environ.get('URL', '')

# hosts = [{"host": HOST, "port": PORT}]

ELASTIC_CAMERA_ID = os.environ.get('ELASTIC_CAMERA_ID', '')
DB_SCHEMA_VERSION = os.environ.get('DB_SCHEMA_VERSION', '')
IMAGE_REPOSITORY = os.environ.get('IMAGE_REPOSITORY', '')
IMAGE_REPOSITORY_VERSION = os.environ.get('IMAGE_REPOSITORY_VERSION', '')
AWS_BATCH_JOB_ID = os.environ.get('AWS_BATCH_JOB_ID', ' ')


camera = LOCATION + '_' + LINE + '_' + CAMERA



OUT_DIR = os.environ.get('OUT_DIR', "name/")  # Output Directory
SECONDS = 1    # Images per seconds
TIMER_RECONNECT = 10.0  # Timer in seconds to failure retry



def keyToDateTime(currKey):

    year, month, day, hour, mins, secs, mil = [int(currKey[:4]), int(currKey[4:6]), int(currKey[6:8]),
                                               int(currKey[9:11]), int(currKey[11:13]), int(currKey[13:15]),
                                               int(currKey[16:22])]
    currTime = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=mins, second=secs,
                                 microsecond=mil)

    return currTime

def UTCToLocalTime(utcTime):

    # convert offset to +/- minutes
    # OFFSET -6 GMT for Flowermound
    return utcTime + datetime.timedelta(hours=-6)



def cleanOutDir(outDir):

    if not os.path.exists(outDir):
        try:
            os.makedirs(outDir)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    if os.path.isdir(outDir):
        shutil.rmtree(outDir)
    os.mkdir(outDir)

# S3 Bucket upload function
def upload_to_Kinesis(file,thing_id,stream):


    try:
        size = os.fstat(file.fileno()).st_size
    except:
        file.seek(0, os.SEEK_END)
        size = file.tell()

    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    payload = {
        'image_id': str(stream),
        # 'image_id': count
    }

    # print (payload)
    put_response = kinesis_client.put_record(
        StreamName=my_stream_name,
        Data=json.dumps(payload),
        PartitionKey=thing_id)
    print(put_response)

    # Rewind for later use

    if put_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print ("success")
        return True
    return False

# Async method to upload to s3 and delete image on local file system
def asynckinesis(directory, filename):

    file = open(os.path.join(directory, filename), 'rb')
    key = file.name
    thing_id="try"
    stream=base64.b64encode(file.read())
    if upload_to_Kinesis(file,thing_id,stream):
        print ('Uploaded   ' + filename)
        os.remove(os.path.join(directory, filename))
        # update_elastic(directory, filename)
        print("Updated", str(os.path.join(directory, filename)) )

    else:
        print ('The upload failed...' + filename)


def URLRequest():
    cap = cv2.VideoCapture()
    cap.open(URL)
    fps = cap.get(cv2.CAP_PROP_FPS)  # Gets the frames per second
    multiplier = fps * SECONDS
    print("in")

    if cap.isOpened():
        while True:
            frameId = int(round(cap.get(
                1)))  # current frame number, rounded b/c sometimes you get frame intervals which aren't integers...this adds a little imprecision but is likely good enough
            success, image = cap.read()

            if success:
                if frameId % multiplier == 0:

                    directory = OUT_DIR + datetime.datetime.now().strftime("%Y-%m-%d") + "/" + camera + "/" + datetime.datetime.now().strftime("%H")  # dir structure - s3 and local
                    filename = "img_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f") + ".jpg"   # filename

                    if not os.path.exists(directory):
                        try:
                            os.makedirs(directory)
                        except OSError as exc:  # Guard against race condition
                            if exc.errno != errno.EEXIST:
                                raise

                    cv2.imwrite(os.path.join(directory, filename), image)
                    print ('Created   ' + filename)

                    Thread(target=asyncKinesis, args=(directory, filename,)).start() 

            else:
                # Failure handing and release existing cap
                cap.release()
                print ("Cap released")
                URLRequest()
                break
    else:
        # Failure handling for access client device
        print("Failed to open Device\n")
        threading.Timer(TIMER_RECONNECT, URLRequest).start()

cleanOutDir(OUT_DIR)

URLRequest()
