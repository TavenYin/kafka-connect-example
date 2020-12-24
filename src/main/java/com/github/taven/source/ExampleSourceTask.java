package com.github.taven.source;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExampleSourceTask extends SourceTask {


    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        System.out.println("ExampleSourceTask started, props:" + props);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        Map<String, Object> sourcePartition = Collections.singletonMap("table", "test");
        Map<String, Object> sourceOffset = Collections.singletonMap("position", 0);
        records.add(new SourceRecord(sourcePartition, sourceOffset, "test_tp", Schema.STRING_SCHEMA, "test_value"));

        return records;
    }

    @Override
    public synchronized void stop() {

    }
}
