### 启动zookeeper 和 kafka
```
docker run -d --name zookeeper -p 2181:2181 -t wurstmeister/zookeeper
docker run -d --name kafka --publish 9092:9092 \
        --link zookeeper \
        --env KAFKA_ZOOKEEPER_CONNECT=192.168.3.21:2181 \
        --env KAFKA_ADVERTISED_HOST_NAME=192.168.3.21 \
        --env KAFKA_ADVERTISED_PORT=9092  \
        wurstmeister/kafka:2.13-2.7.0
```