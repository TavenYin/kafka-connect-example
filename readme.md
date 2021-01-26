### 1. 启动zookeeper 和 kafka
```
# script/docker-compose.yaml 启动zookeeper与kafka
docker-compose -f docker-compose.yaml up
```

### 2. 运行项目中内嵌的Kafka Connect
```
mvn clean install -DskipTests -f pom.xml
java -cp ...
```
或者直接在IDE中执行

### 3. 启动Connector
```
# script 目录下执行该命令
# 启动 source Connector
cd script
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @source.json

# 删除Connector
curl -X DELETE localhost:8083/connectors/example-source

# 查看Connector
curl -X GET localhost:8083/connectors/example-source/status
```