mvn clean install -DskipTests -f pom.xml
cd target
scp kafka-connect-example-1.0-SNAPSHOT.jar root@192.168.235.128:/opt/connectors