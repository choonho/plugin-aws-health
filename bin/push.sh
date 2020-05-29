#!/usr/bin/env bash
# How to upload
./build.sh
docker push pyengine/aws-health:1.0
docker push spaceone/aws-health:1.0
