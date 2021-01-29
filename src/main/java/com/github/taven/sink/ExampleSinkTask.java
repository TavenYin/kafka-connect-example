package com.github.taven.sink;

import com.alibaba.fastjson.JSONObject;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ExampleSinkTask extends SinkTask {
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    SinkConfig config;
    RedisClient redisClient;
    StatefulRedisConnection<String, String> connection;

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
                JSONObject redisVal = new JSONObject();

                record.valueSchema().fields().forEach(field -> {
                    redisVal.put(field.name(), value.get(field));
                });

                RedisFuture<String> set = async.set(redisKey, redisVal.toJSONString());
                futures.add(set);
            }

        });

        Futures.awaitAll(Duration.ofSeconds(1), futures.toArray(new RedisFuture[0]));

    }

    @Override
    public void stop() {
        connection.close();
        redisClient.shutdown();
    }
}
