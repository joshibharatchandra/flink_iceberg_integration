package com.example.flinkiceberg.controller;



import com.example.flinkiceberg.service.FlinkIcebergService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
@RestController
public class FlinkController {



    @Autowired
    private  final FlinkIcebergService flinkIcebergService;

    @Autowired // Dependency injection using constructor-based injection
    public FlinkController(FlinkIcebergService flinkIcebergService) {
        this.flinkIcebergService = flinkIcebergService;
    }




    @GetMapping("/runFlinkJob")
    public String runFlinkJob() {
        try {
            flinkIcebergService.executeFlinkJob();
            return "Flink Iceberg job executed successfully!";
        } catch (Exception e) {
            return "Error executing Flink Iceberg job: " + e.getMessage();
        }
    }
}


