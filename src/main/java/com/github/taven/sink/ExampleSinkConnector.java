package com.github.taven.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExampleSinkConnector extends SinkConnector {
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    private SinkConfig config;
    private Map<String, String> configMap;

    @Override
    public void start(Map<String, String> props) {
        this.configMap = props;
        this.config = new SinkConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ExampleSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return Collections.singletonList(configMap);
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return SinkConfig.SINK_CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
