#!/bin/bash

javac -cp ./derbyclient.jar: ./ClusterInfo.java  RunDDL.java
java -cp .:derbyclient.jar RunDDL $1 $2
#java -cp .:derbyclient.jar RunDDL ./clustercfg.cfg ./sqlfile.sql