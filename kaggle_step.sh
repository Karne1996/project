#!/bin/bash

cd /home/hadoop/
mkdir /home/hadoop/.kaggle
sudo cp /home/hadoop/kaggle.json /home/hadoop/.kaggle/

/usr/local/bin/pip3.6 install --user kaggle
/usr/local/bin/pip3.6 install kaggle
~/.local/bin/kaggle datasets download -d yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018 --path /home/hadoop/Flight_logs --unzip
hadoop fs -put Flight_logs

~/.local/bin/kaggle datasets download -d usdot/flight-delays --path /home/hadoop/Airport_airline --unzip
hadoop fs -put Airport_airline


s3-dist-cp --src /user/hadoop/Airport_airline  --dest s3a://finalgroup6/Raw_Data/

s3-dist-cp --src /user/hadoop/Flight_logs --dest s3a://finalgroup6/Raw_Data/Data/

cd Flight_logs/
hdfs dfs -put 2017.csv s3a://finalgroup6/Raw_Data/RDS/
hdfs dfs -put 2018.csv s3a://finalgroup6/Raw_Data/INC/

hdfs dfs -rm s3a://finalgroup6/Raw_Data/Data/2017.csv
hdfs dfs -rm s3a://finalgroup6/Raw_Data/Data/2018.csv

cd ..

wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.45.tar.gz

sudo tar -xzf mysql-connector-java-5.1.45.tar.gz

sudo cp mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar /usr/lib/spark/jars


