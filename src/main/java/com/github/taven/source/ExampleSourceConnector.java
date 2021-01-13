package com.github.taven.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExampleSourceConnector extends SourceConnector {

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        System.out.println("ExampleSourceConnector started, props:" + props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ExampleSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.singletonList(props);
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        ConfigDef configDef = new ConfigDef();
        configDef.define("database.url", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "url doc")
                .define("database.username", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "username doc")
                .define("database.password", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "password doc");
        return configDef;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
