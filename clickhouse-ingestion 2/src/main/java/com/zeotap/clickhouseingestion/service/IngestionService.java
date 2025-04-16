package com.zeotap.clickhouseingestion.service;

import com.zeotap.clickhouseingestion.dto.ClickHouseConfig;
import com.zeotap.clickhouseingestion.dto.FlatFileConfig;
import com.zeotap.clickhouseingestion.dto.IngestionResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;

@Service
public class IngestionService {

    @Autowired
    private ClickHouseService clickHouseService;

    @Autowired
    private FlatFileService flatFileService;

    public IngestionResult ingestClickHouseToFlatFile(ClickHouseConfig config, String table, List<String> selectedColumns) throws Exception {
        IngestionResult result = new IngestionResult();
        
        // Query data from ClickHouse
        List<List<String>> data = clickHouseService.queryData(config, table, selectedColumns);
        
        // Write data to CSV file
        File outputFile = flatFileService.writeToFile(selectedColumns, data, ',');
        
        // Copy file to user's Downloads directory
        String userHome = System.getProperty("user.home");
        Path downloadDir = Path.of(userHome, "Downloads");
        String fileName = table + "_export.csv";
        Path targetPath = downloadDir.resolve(fileName);
        
        Files.copy(outputFile.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING);
        
        // Set result
        result.setRecordsProcessed(data.size());
        result.setMessage("Data exported to " + targetPath);
        
        return result;
    }
    
    public IngestionResult ingestFlatFileToClickHouse(FlatFileConfig fileConfig, ClickHouseConfig chConfig, List<String> selectedColumns) throws Exception {
        IngestionResult result = new IngestionResult();
        
        // Read data from flat file
        List<List<String>> data = flatFileService.readData(fileConfig, selectedColumns);
        
        // Create a temporary table in ClickHouse if it doesn't exist
        String tempTable = "temp_import_" + System.currentTimeMillis();
        createTemporaryTable(chConfig, tempTable, selectedColumns);
        
        // Insert data into ClickHouse
        int recordsInserted = clickHouseService.insertData(chConfig, tempTable, selectedColumns, data);
        
        // Clean up
        flatFileService.cleanup();
        
        // Set result
        result.setRecordsProcessed(recordsInserted);
        result.setMessage("Data imported to table " + tempTable + " in database " + chConfig.getDatabase());
        
        return result;
    }
    
    private void createTemporaryTable(ClickHouseConfig config, String tableName, List<String> columns) throws Exception {
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseNode node = createNode(config);
            
            StringBuilder createTableQuery = new StringBuilder();
            createTableQuery.append("CREATE TABLE IF NOT EXISTS ")
                    .append(config.getDatabase())
                    .append(".")
                    .append(tableName)
                    .append(" (");
            
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    createTableQuery.append(", ");
                }
                createTableQuery.append(columns.get(i)).append(" String");
            }
            
            createTableQuery.append(") ENGINE = MergeTree() ORDER BY tuple()");
            
            client.read(node).query(createTableQuery.toString()).execute();
        }
    }

    private ClickHouseNode createNode(ClickHouseConfig config) {
        // Use username and password instead of JWT
        ClickHouseCredentials credentials = ClickHouseCredentials.fromUserAndPassword(
                config.getUser(), config.getPassword());

        // Default to HTTP protocol
        return ClickHouseNode.builder()
                .host(config.getHost())
                .port(config.getPort())
                .database(config.getDatabase())
                .credentials(credentials)
                .build();
    }
} 