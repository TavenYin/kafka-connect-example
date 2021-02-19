package com.github.taven.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.internal.Futures;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.time.Duration;
import java.util.*;

public class ExampleSinkTask extends SinkTask {
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    SinkConfig config;
    RedisClient redisClient;
    StatefulRedisConnection<String, String> connection;
    static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void start(Map<String, String> props) {
        config = new SinkConfig(props);

        redisClient = RedisClient.create(
                RedisURI.builder()
                .withHost(config.getString(SinkConfig.REDIS_HOST))
                .withPort(config.getInt(SinkConfig.REDIS_PORT))
                .withDatabase(config.getInt(SinkConfig.REDIS_DATABASE))
                .withPassword(config.getString(SinkConfig.REDIS_PASSWORD).toCharArray())
                .build()
        );

        connection = redisClient.connect();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        RedisStringAsyncCommands<String, String> async = connection.async();

        List<RedisFuture<String>> futures = new ArrayList<>();

        records.forEach(record -> {
            // 这里简单做一下，直接按照source的结构解析了
            if (record.valueSchema().type() == Schema.Type.STRUCT
                    && record.keySchema().type() == Schema.Type.STRING) {

                String key = (String) record.key();
                String redisKey = record.topic() + "_" + key;

                Struct value = (Struct) record.value();
                Map<String, Object> redisVal = new HashMap<>();

                record.valueSchema().fields().forEach(field -> {
                    redisVal.put(field.name(), value.get(field));
                });

                String jsonVal;
                try {
                    jsonVal = objectMapper.writeValueAsString(redisVal);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e.getMessage());
                }

                RedisFuture<String> set = async.set(redisKey, jsonVal);
                futures.add(set);
            }

        });

        Futures.awaitAll(Duration.ofSeconds(2), futures.toArray(new RedisFuture[0]));

    }

    @Override
    public void stop() {
        connection.close();
        redisClient.shutdown();
    }
}
