package com.github.taven.source;

import com.mysql.cj.MysqlType;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class ExampleSourceTask extends SourceTask {
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    ExampleSourceConfig config;
    Connection connection;
    String currentTable;
    PreparedStatement stmt;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new ExampleSourceConfig(props);
        currentTable = props.get("database.table");

        loadJdbcDriver();

        connection = getJdbcConnection();

        logger.info("ExampleSourceTask starting, props:{}", props);
    }

    private void loadJdbcDriver() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }


    private Connection getJdbcConnection() {
        try {
            // 根据config
            return DriverManager.getConnection(config.getString("database.url"),
                    config.getString("database.username"), config.getString("database.password"));
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        logger.info("do poll");

        ResultSet resultSet = null;

        try {
            getStatement();

            resultSet = stmt.executeQuery();

            List<SourceRecord> records = resultSetConvert(resultSet);

            if (records != null)
                return records;

        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);

        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return null;
    }


    private void getStatement() throws SQLException {
        if (stmt == null) {
            String sql = String.format("select * from %s where id > ? limit 10000", currentTable);
            stmt = connection.prepareStatement(sql);
        }

        // TODO 参考jdbc-connect，由于offset的提交是异步的，所以并不能依赖该方法读取offset，该方法是用于task开始时，获取上一次任务的offset
        Map<String, Object> sourceOffsetRead = context.offsetStorageReader()
                .offset(Collections.singletonMap("currentTable", this.currentTable));
        Long position = sourceOffsetRead != null ? (Long) sourceOffsetRead.get("position") : Long.valueOf(0);

        stmt.setInt(1, position.intValue());
    }

    private List<SourceRecord> resultSetConvert(ResultSet resultSet) throws SQLException {
        if (resultSet != null) {
            List<SourceRecord> records = new ArrayList<>();

            while (resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();

                int id = resultSet.getInt("id");

                Struct struct = resultSetMapping(resultSet, metaData);

                Map<String, Object> sourcePartition = Collections.singletonMap("currentTable", currentTable);
                Map<String, Object> sourceOffset = Collections.singletonMap("position", id);

                records.add(new SourceRecord(sourcePartition, sourceOffset, currentTable, Schema.STRING_SCHEMA,
                        String.valueOf(id), struct.schema(), struct));

            }

            return records;
        }
        return null;
    }

    /**
     * 将ResultSet 结构映射到 Kafka Connect Struct
     *
     * @param resultSet
     * @param metaData
     * @return
     * @throws SQLException
     */
    private Struct resultSetMapping(ResultSet resultSet, ResultSetMetaData metaData) throws SQLException {
        Map<String, Object> fieldValueMap = new HashMap<>();

        SchemaBuilder builder = SchemaBuilder.struct().name(currentTable);

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            int columnType = metaData.getColumnType(i);
            MysqlType mysqlType = MysqlType.getByJdbcType(columnType);

            Schema columnSchema;
            Object value;

            // 这里只列出几种类型，用于演示
            switch (mysqlType) {
                case INT:
                    columnSchema = Schema.INT32_SCHEMA;
                    value = resultSet.getInt(i);
                    break;
                case VARCHAR:
                case CHAR:
                case JSON:
                case TEXT:
                case MEDIUMTEXT:
                case LONGTEXT:
                    columnSchema = Schema.STRING_SCHEMA;
                    value = resultSet.getString(i);
                    break;
                case DECIMAL:
                    columnSchema = Schema.STRING_SCHEMA;
                    value = resultSet.getBigDecimal(i);
                    break;
                case DATE:
                    columnSchema = Schema.INT64_SCHEMA;
                    value = resultSet.getDate(i).getTime();
                    break;
                case BIGINT:
                    columnSchema = Schema.INT64_SCHEMA;
                    value = resultSet.getLong(i);
                    break;
                case BIT:
                    columnSchema = Schema.BOOLEAN_SCHEMA;
                    value = resultSet.getBoolean(i);
                    break;
                case DOUBLE:
                    columnSchema = Schema.FLOAT64_SCHEMA;
                    value = resultSet.getDouble(i);
                    break;
                default:
                    throw new RuntimeException("not supported type");
            }

            String filedName = metaData.getColumnName(i);

            fieldValueMap.put(filedName, value);

            builder.field(filedName, columnSchema);
        }

        Schema schema = builder.build();
        Struct struct = new Struct(schema);

        for (Field field : schema.fields()) {
            struct.put(field, fieldValueMap.get(field.name()));
        }

        return struct;
    }


    @Override
    public synchronized void stop() {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public static void main(String[] args) {
        Map map = new HashMap();
        map.put("a", "a");

        Map map2 = new HashMap();
        map2.put("a", "a");
        System.out.println(map.equals(map2));
    }

}
