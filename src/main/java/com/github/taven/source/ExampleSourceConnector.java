package com.github.taven.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ExampleSourceConnector extends SourceConnector {
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    private ExampleSourceConfig config;
    private Map<String, String> configMap;

    @Override
    public void start(Map<String, String> props) {
        this.configMap = props;
        this.config = new ExampleSourceConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ExampleSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> tables = config.getList("database.tables");

        // 这里简单的按照表数量进行分组。实际情况中，可以根据maxTask以及其他配置来决定Task数量
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        for (String table : tables) {
            Map<String, String> taskConfig = new HashMap<>(configMap);
            taskConfig.put("database.table", table);
            taskConfigs.add(taskConfig);
        }

        return taskConfigs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return ExampleSourceConfig.CONFIGDEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
