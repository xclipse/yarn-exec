call mvn package -DskipTests 
scp yarn-exec-0.0.1-SNAPSHOT.jar hadoop@h1:/home/hadoop/test.jar 
