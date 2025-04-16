package com.zeotap.clickhouseingestion.service;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseFormat;
import com.zeotap.clickhouseingestion.dto.ClickHouseConfig;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ClickHouseService {

    public List<String> getTables(ClickHouseConfig config) throws Exception {
        List<String> tables = new ArrayList<>();
        
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseNode node = createNode(config);
            ClickHouseRequest<?> request = client.read(node)
                    .format(ClickHouseFormat.TSV)
                    .query("SHOW TABLES FROM " + config.getDatabase());
            
            try (ClickHouseResponse response = request.execute().get()) {
                response.records().forEach(record -> {
                    tables.add(record.getValue(0).asString());
                });
            }
        }
        
        return tables;
    }
    
    public List<String> getColumns(ClickHouseConfig config, String table) throws Exception {
        List<String> columns = new ArrayList<>();
        
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseNode node = createNode(config);
            ClickHouseRequest<?> request = client.read(node)
                    .format(ClickHouseFormat.TSV)
                    .query("DESCRIBE TABLE " + config.getDatabase() + "." + table);

            try (ClickHouseResponse response = request.execute().get()) {
                response.records().forEach(record -> {
                    columns.add(record.getValue(0).asString());
                });
            }
        }
        
        return columns;
    }
    
    public List<List<String>> queryData(ClickHouseConfig config, String table, List<String> columns) throws Exception {
        List<List<String>> data = new ArrayList<>();
        
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseNode node = createNode(config);
            
            String columnsStr = String.join(", ", columns);
            String query = "SELECT " + columnsStr + " FROM " + config.getDatabase() + "." + table;
            
            ClickHouseRequest<?> request = client.read(node)
                    .format(ClickHouseFormat.TSV)
                    .query(query);

            try (ClickHouseResponse response = request.execute().get()) {
                response.records().forEach(record -> {
                    List<String> row = new ArrayList<>();
                    for (int i = 0; i < columns.size(); i++) {
                        row.add(record.getValue(i).asString());
                    }
                    data.add(row);
                });
            }
        }
        
        return data;
    }
    
    public int insertData(ClickHouseConfig config, String table, List<String> columns, List<List<String>> data) throws Exception {
        int recordsInserted = 0;
        
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseNode node = createNode(config);
            
            String columnsStr = String.join(", ", columns);
            StringBuilder valuesBuilder = new StringBuilder();
            
            for (List<String> row : data) {
                if (valuesBuilder.length() > 0) {
                    valuesBuilder.append(", ");
                }
                
                valuesBuilder.append("(");
                for (int i = 0; i < row.size(); i++) {
                    if (i > 0) {
                        valuesBuilder.append(", ");
                    }
                    valuesBuilder.append("'").append(row.get(i).replace("'", "''")).append("'");
                }
                valuesBuilder.append(")");
                
                // Execute in batches to avoid huge queries
                if (recordsInserted % 1000 == 0 && valuesBuilder.length() > 0) {
                    String query = "INSERT INTO " + config.getDatabase() + "." + table + 
                            " (" + columnsStr + ") VALUES " + valuesBuilder;
                    
                    client.read(node).query(query).execute();
                    valuesBuilder = new StringBuilder();
                }
                
                recordsInserted++;
            }
            
            // Insert any remaining records
            if (valuesBuilder.length() > 0) {
                String query = "INSERT INTO " + config.getDatabase() + "." + table + 
                        " (" + columnsStr + ") VALUES " + valuesBuilder;
                
                client.read(node).query(query).execute();
            }
        }
        
        return recordsInserted;
    }

    private ClickHouseNode createNode(ClickHouseConfig config) {
        // Use username and password instead of JWT
        ClickHouseCredentials credentials = ClickHouseCredentials.fromUserAndPassword(
                config.getUser(), config.getPassword());

        // Set the protocol using the correct method
        return ClickHouseNode.builder()
                .host(config.getHost())
                .port(ClickHouseProtocol.HTTP, config.getPort())
                .database(config.getDatabase())
                .credentials(credentials)
                .build();
    }
} 