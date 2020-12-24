package com.github.taven.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;

public class ExampleSinkConnector extends SinkConnector {
    @Override
    public void start(Map<String, String> map) {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return ExampleSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
