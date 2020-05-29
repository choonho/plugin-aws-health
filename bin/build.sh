#! /bin/bash
# Build a docker image
cd ..
docker build -t pyengine/aws-health .
docker tag pyengine/aws-health pyengine/aws-health:1.0
docker tag pyengine/aws-health spaceone/aws-health:1.0
