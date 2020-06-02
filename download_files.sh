#!/bin/bash

aws s3 cp s3://mm-music-data/config/* /home/hadoop/.aws/config/
aws s3 cp s3://mm-music-data/credentials /home/hadoop/.aws/

sudo pip3 install boto3
sudo pip3 install pandas