#!/bin/bash

javac -cp ./derbyclient.jar: ./ClusterInfo.java  RunSQL.java
java -cp .:derbyclient.jar RunSQL $1 $2
#java -cp .:derbyclient.jar RunDDL ./clustercfg.cfg ./sqlfile.sql