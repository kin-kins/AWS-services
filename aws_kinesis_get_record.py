import boto3
import json
from datetime import datetime
import time
import base64


def main():
    my_stream_name = 'new_try'

    kinesis_client = boto3.client('kinesis', region_name='us-east-1')

    response = kinesis_client.describe_stream(StreamName=my_stream_name)

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']


    shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                       ShardId=my_shard_id,
                                                       ShardIteratorType='AT_SEQUENCE_NUMBER',StartingSequenceNumber='49598685317897772500072500026065375050514518896113876994',)

    my_shard_iterator = shard_iterator['ShardIterator']

    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator)
    records=(record_response['Records'])
    count=0
    for record in records:
        t_record=(str(record['Data'])[16:-5]+'\'')
        t_record=t_record[0:1]+t_record[2:]
        t_record=t_record[2:-1]
        #print(t_record)
       # print("\n")
        fh = open("/Users/ashukumar/Desktop/kin/"+str(count)+".jpg", "wb")
        fh.write(base64.b64decode(t_record))
        fh.close()
        count+=1
    # while 'NextShardIterator' in record_response:
    #     record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],Limit=2)
    #     print (record_response)
    # wait for 5 seconds
        #time.sleep(5)

if __name__ == '__main__':
    main()
