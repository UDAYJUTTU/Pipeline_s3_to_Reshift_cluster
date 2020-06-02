from boto3 import client
from sys import argv

try:
  clusterId=argv[1]
  script=argv[2]
except:
  print("Syntax: librariesSsm.py [ClusterId] [S3_Script_Path]")
  import sys
  sys.exit(1)

emrclient=client('emr')

instances=emrclient.list_instances(ClusterId=clusterId,InstanceGroupTypes=['CORE'])['Instances']
instance_list=[x['Ec2InstanceId'] for x in instances]

ec2client=client('ec2')
ec2client.create_tags(Resources=instance_list,Tags=[{"Key":"environment","Value":"coreNodeLibs"}])

ssmclient=client('ssm')

command = "aws s3 cp " + script + " /home/hadoop"
try:
  first_command=ssmclient.send_command(Targets=[{"Key":"tag:environment","Values":["coreNodeLibs"]}],
                  DocumentName='AWS-RunShellScript',
                  Parameters={"commands":[command]}, 
                  TimeoutSeconds=3600)['Command']['CommandId']
  
  import time
  time.sleep(15)

  first_command_status=ssmclient.list_commands(
      CommandId=first_command,
      Filters=[
          {
              'key': 'Status',
              'value': 'SUCCESS'
          },
      ]
  )['Commands'][0]['Status']

  second_command=""
  second_command_status=""
  

  if (first_command_status=='Success'):

    second_command=ssmclient.send_command(Targets=[{"Key":"tag:environment","Values":["coreNodeLibs"]}],
      DocumentName='AWS-RunShellScript',
      Parameters={"commands":["bash /home/hadoop/install_libraries.sh"]}, 
      TimeoutSeconds=3600)['Command']['CommandId']
    
    second_command_status=ssmclient.list_commands(
      CommandId=first_command,
      Filters=[
          {
              'key': 'Status',
              'value': 'SUCCESS'
          },
      ]
    )['Commands'][0]['Status']
    time.sleep(30)
    print("First command, " + first_command + ": " + first_command_status)
    print("Second command:" + second_command + ": " + second_command_status)

except Exception as e:
  print(e)
