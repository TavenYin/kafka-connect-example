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
            return DriverManager.getConnection("jdbc:mysql://localhost/test?" +
                    "user=minty&password=greatsqldb");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return queryRecord();
    }

    private List<SourceRecord> queryRecord() {
        ResultSet resultSet = null;

        try {
            if (stmt == null) {
                String sql = "select * from ? where id > ? limit 10000";
                stmt = connection.prepareStatement(sql);
            }

            Map<String, Object> sourceOffsetRead = context.offsetStorageReader()
                    .offset(Collections.singletonMap("currentTable", this.currentTable));
            Integer position = (Integer) sourceOffsetRead.get("position");

            stmt.setString(1, this.currentTable);
            stmt.setInt(2, position);

            resultSet = stmt.executeQuery();

            if (resultSet != null) {
                List<SourceRecord> records = new ArrayList<>();

                while (resultSet.next()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();

                    int id = resultSet.getInt("id");

                    Struct struct = handleResultSet(resultSet, metaData);

                    Map<String, Object> sourcePartition = Collections.singletonMap("currentTable", currentTable);
                    Map<String, Object> sourceOffset = Collections.singletonMap("position", id);

                    records.add(new SourceRecord(sourcePartition, sourceOffset, currentTable, Schema.STRING_SCHEMA,
                            String.valueOf(id), struct.schema(), struct));

                }

                return records;
            }

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

    private Struct handleResultSet(ResultSet resultSet, ResultSetMetaData metaData) throws SQLException {
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
                case SMALLINT:
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
                case FLOAT:
                    columnSchema = Schema.FLOAT64_SCHEMA;
                    value = resultSet.getDouble(i);// 这里貌似有点奇怪。。。
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
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }


    public static void main(String[] args) {
        System.out.println(String.class.getName());
    }

}
