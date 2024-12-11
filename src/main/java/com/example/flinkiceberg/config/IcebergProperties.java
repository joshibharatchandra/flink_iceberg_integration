package com.example.flinkiceberg.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "iceberg")
public class IcebergProperties {

    private String catalogName;
    private String warehousePath;
    private String schemaName;
    private String tableName;


    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getWarehousePath() {
        return warehousePath;
    }

    public void setWarehousePath(String warehousePath) {
        this.warehousePath = warehousePath;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
