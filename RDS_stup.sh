#!/bin/bash

cd /home/hadoop/

wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.45.tar.gz

tar -xzf mysql-connector-java-5.1.45.tar.gz

cp mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar /usr/lib/spark/jars


