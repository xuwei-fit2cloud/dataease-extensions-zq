#!/bin/sh
mvn clean package -U -Dmaven.test.skip=true

cp sls-backend/target/sls-backend-1.0-SNAPSHOT-jar-with-dependencies.jar ./sls-backend-1.0-SNAPSHOT.jar

zip -r sls.zip  ./sls-backend-1.0-SNAPSHOT.jar ./slsDriver ./plugin.json
