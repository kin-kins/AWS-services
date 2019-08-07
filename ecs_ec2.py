import boto3
from boto.s3.key import Key

def create_ecs_instance():
    client = boto3.client('ec2',
                          aws_access_key_id='',
                          aws_secret_access_key='',
                          region_name='')
    response = client.run_instances(
        # Use the official ECS image
        ImageId='ami-0fac5486e4cff37f4',# for more information you can visit official site for ECS optimized images
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",# choose according to your design
        KeyName='ec2_2k19',
        IamInstanceProfile={
            "Name": "ecsInstanceRole"
        },
        UserData="#!/bin/bash \n echo ECS_CLUSTER=" + "ENTER YOUR UNMANGED BATCH COMPUTE NAME" + " >> /etc/ecs/ecs.config",
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name', # ENTER THE NAME OF THE SERVER
                        'Value': 'la'
                    },
                ]
            },
        ],
        NetworkInterfaces=[
            {
                'SubnetId': 'subnet-07ed0260',  # choose your subnet
                'DeviceIndex': 0,
                'AssociatePublicIpAddress': True,
                'Groups': ['sg-157fda5c']
            }
        ]
    )
    print(response)
   # print("wau")


def main():
    create_ecs_instance()


if __name__ == '__main__':
    main()
