#!/bin/sh
mvn clean package -U -Dmaven.test.skip=true

cp influxdb-backend/target/influxdb-backend-1.18.6-jar-with-dependencies.jar ./influxdb-backend-1.18.6.jar

zip -r influxdb.zip  ./influxdb-backend-1.18.6.jar ./influxdbDriver ./plugin.json
