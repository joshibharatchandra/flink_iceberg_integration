package com.example.flinkiceberg.service;

import com.example.flinkiceberg.config.IcebergProperties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class FlinkIcebergService {

    private final IcebergProperties icebergProperties;

    @Value("${google.application.credentials}")
    private String googleCredentialsPath;

    public FlinkIcebergService(IcebergProperties icebergProperties) {
        this.icebergProperties = icebergProperties;
    }

    public void executeFlinkJob() throws Exception {
        // Set Google Cloud credentials path
        System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", googleCredentialsPath);

        // Initialize Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Prepare sample data
        DataStream<MyDTO> dataStream = env.fromElements(
                new MyDTO(1, "Bharat", "20240101"),
                new MyDTO(2, "Manish", "20240102"),
                new MyDTO(3, "Mayank", "20240103")
        );

        // Register the DataStream as a temporary table
        tableEnv.createTemporaryView("myDTO", dataStream);

        // Create Iceberg table using properties
        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s.%s (\n" +
                        "  id INT, \n" +
                        "  name STRING, \n" +
                        "  dob STRING \n" +
                        ") WITH (\n" +
                        "  'connector' = 'iceberg', \n" +
                        "  'catalog-name' = '%s', \n" +
                        "  'catalog-type' = 'hadoop', \n" +
                        "  'warehouse' = '%s', \n" +
                        "  'format' = 'avro' \n" +
                        ")",
                icebergProperties.getCatalogName(),
                icebergProperties.getSchemaName(),
                icebergProperties.getTableName(),
                icebergProperties.getCatalogName(),
                icebergProperties.getWarehousePath()
        );

        tableEnv.executeSql(createTableQuery);

        // Insert data into the Iceberg table
        String insertQuery = String.format(
                "INSERT INTO %s.%s.%s SELECT * FROM myDTO",
                icebergProperties.getCatalogName(),
                icebergProperties.getSchemaName(),
                icebergProperties.getTableName()
        );

        TableResult insertResult = tableEnv.executeSql(insertQuery);

        // Wait for Flink job to finish
        insertResult.await();
    }

    public static class MyDTO {
        public int id;
        public String name;
        public String dob;

        public MyDTO() {}

        public MyDTO(int id, String name, String dob) {
            this.id = id;
            this.name = name;
            this.dob = dob;
        }

        @Override
        public String toString() {
            return "MyDTO{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", dob='" + dob + '\'' +
                    '}';
        }
    }
}
