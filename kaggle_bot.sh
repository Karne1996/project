#!/bin/bash

sudo ln -fs /usr/bin/python3.6 /etc/alternatives/python
sudo ln -fs /usr/bin/pip-3.6 /etc/alternatives/pip
sudo pip install --upgrade pip 
sudo pip install kaggle-cli
sudo pip install kaggle

aws s3 cp s3://finalgroup6/kaggle.json /home/hadoop/kaggle.json


