package com.github.taven.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class SinkConfig extends AbstractConfig {
    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";
    public static final String REDIS_DATABASE = "redis.database";
    public static final String REDIS_PASSWORD = "redis.password";

    public static final String REDIS_HOST_DOC = "REDIS_HOST_DOC";
    public static final String REDIS_PORT_DOC = "REDIS_PORT_DOC";
    public static final String REDIS_DATABASE_DOC = "REDIS_DATABASE_DOC";
    public static final String REDIS_PASSWORD_DOC = "REDIS_PASSWORD_DOC";

    public SinkConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps, boolean doLog) {
        super(definition, originals, configProviderProps, doLog);
    }

    public SinkConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public SinkConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        super(definition, originals, doLog);
    }

    public SinkConfig(Map<?, ?> config) {
        super(SINK_CONFIG_DEF, config);
    }

    static final ConfigDef SINK_CONFIG_DEF = new ConfigDef()
            .define(REDIS_HOST, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, REDIS_HOST_DOC)
            .define(REDIS_PORT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, REDIS_PORT_DOC)
            .define(REDIS_DATABASE, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, REDIS_DATABASE_DOC)
            .define(REDIS_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, REDIS_PASSWORD_DOC);

}
