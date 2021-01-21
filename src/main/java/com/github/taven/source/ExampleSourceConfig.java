package com.github.taven.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ExampleSourceConfig extends AbstractConfig {
    public ExampleSourceConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps, boolean doLog) {
        super(definition, originals, configProviderProps, doLog);
    }

    public ExampleSourceConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public ExampleSourceConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        super(definition, originals, doLog);
    }


    public ExampleSourceConfig(Map<String, String> config) {
        super(CONFIGDEF, config);
    }

    static ConfigDef CONFIGDEF = new ConfigDef()
            .define("database.url", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "url doc")
            .define("database.username", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "username doc")
            .define("database.password", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "password doc");

}
