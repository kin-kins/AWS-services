import boto3
import json
import base64

my_stream_name = 'new_try'

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def put_to_stream(thing_id,b64,count):
    payload = {
                'image_id': str(b64),
                #'image_id': count
              }

    #print (payload)
    put_response = kinesis_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(payload),
                        PartitionKey=thing_id)
    print (put_response)


def main():
    image_id = 0

    while True:
        thing_id = 'CAMERA-001'
        with open("/Users/ashukumar/Desktop/Standard/ASHUimgs/"+str(image_id)+".jpg","rb") as imageFile:
            stream = base64.b64encode(imageFile.read())
        # print(stream)
        # t=str(stream)
        # t=t[2:-1]
        # print('\n')
        # print(t)
        # fh = open("/Users/ashukumar/Desktop/kin/" + str(image_id) + ".jpg", "wb")
        # fh.write(base64.b64decode(t))
        # fh.close()
        #print("/Users/ashukumar/Desktop/Standard/ASHUimgs/" + str(image_id) + ".jpg")
        #print(stream)
        put_to_stream(thing_id,stream,image_id)
        image_id+=1
        #print(image_id)
        if image_id>389:
            break

if __name__=='__main__' :
    main()
