package com.example.flinkiceberg;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.example.flinkiceberg.config.IcebergProperties;

@SpringBootApplication
@EnableConfigurationProperties(IcebergProperties.class)
public class FlinkIcebergApplication {
	public static void main(String[] args) {
		SpringApplication.run(FlinkIcebergApplication.class, args);
	}
}
