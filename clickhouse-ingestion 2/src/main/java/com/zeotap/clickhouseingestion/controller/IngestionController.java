package com.zeotap.clickhouseingestion.controller;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.zeotap.clickhouseingestion.dto.ClickHouseConfig;
import com.zeotap.clickhouseingestion.dto.FlatFileConfig;
import com.zeotap.clickhouseingestion.dto.IngestionResult;
import com.zeotap.clickhouseingestion.service.ClickHouseService;
import com.zeotap.clickhouseingestion.service.FlatFileService;
import com.zeotap.clickhouseingestion.service.IngestionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Controller
public class IngestionController {

    @Autowired
    private ClickHouseService clickHouseService;

    @Autowired
    private FlatFileService flatFileService;

    @Autowired
    private IngestionService ingestionService;

    @GetMapping("/")
    public String index(Model model) {
        return "index";
    }

    @PostMapping("/ingest")
    public String handleIngestion(
            @RequestParam("direction") String direction,
            @RequestParam(value = "action", required = false) String action,
            @RequestParam(value = "host", required = false) String host,
            @RequestParam(value = "port", required = false, defaultValue = "0") int port,
            @RequestParam(value = "database", required = false) String database,
            @RequestParam(value = "user", required = false) String user,
            @RequestParam(value = "tae~VfBI.2LDE", required = false) String password,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "selectedColumns", required = false) List<String> selectedColumns,
            @RequestParam(value = "file", required = false) MultipartFile file,
            @RequestParam(value = "delimiter", required = false, defaultValue = ",") String delimiter,
            @RequestParam(value = "protocol", required = false) String protocol,
            Model model) {

        model.addAttribute("direction", direction);

        try {
            // ClickHouse to Flat File flow
            System.out.println("user"+ user);
            if ("CH_TO_FF".equals(direction)) {
                ClickHouseConfig config = new ClickHouseConfig();
                config.setHost(host);
                config.setPort(port);
                config.setDatabase(database);
                config.setUser(user);
                config.setPassword(password);
                config.setProtocol(protocol);
                model.addAttribute("clickHouseConfig", config);
                if ("connect".equals(action)) {
                    config.setProtocol(protocol != null ? protocol : "https"); // SET IT FIRST

                    String endpoint = String.format("%s://%s:%d", config.getProtocol(), config.getHost(), config.getPort());

                    Client client = new Client.Builder()
                            .addEndpoint(endpoint)
                            .setUsername(config.getUser())
                            .setPassword(config.getPassword())
                            .build();

                    final String sql = "select * from nyc_taxi LIMIT 10";

// Default format is RowBinaryWithNamesAndTypesFormatReader so reader have all information about columns
                    try (QueryResponse response = client.query(sql).get(3, TimeUnit.SECONDS);) {

                        // Create a reader to access the data in a convenient way
                        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

                        while (reader.hasNext()) {
                            reader.next(); // Read the next record from stream and parse it

                            // get values
                            double id = reader.getDouble("trip_id");
                            System.out.println("ID is "+id);

                            // collecting data
                        }
                    } catch (Exception e) {
                        System.out.println("Failed to read data"+ e);
                    }

                    // List<String> tables = clickHouseService.getTables(config);
                    //model.addAttribute("tables", tables);
                } else if ("loadColumns".equals(action)) {
                    List<String> columns = clickHouseService.getColumns(config, table);
                    model.addAttribute("columns", columns);
                    model.addAttribute("selectedTable", table);
                    model.addAttribute("tables", clickHouseService.getTables(config));
                } else if ("startIngestion".equals(action)) {
                    if (selectedColumns == null || selectedColumns.isEmpty()) {
                        model.addAttribute("errorMessage", "Please select at least one column");
                        List<String> columns = clickHouseService.getColumns(config, table);
                        model.addAttribute("columns", columns);
                        model.addAttribute("selectedTable", table);
                        model.addAttribute("tables", clickHouseService.getTables(config));
                        return "index";
                    }
                    
                    IngestionResult result = ingestionService.ingestClickHouseToFlatFile(config, table, selectedColumns);
                    model.addAttribute("successMessage", "Ingestion completed: " + result.getRecordsProcessed() + 
                            " records processed. " + result.getMessage());
                }
            }
            // Flat File to ClickHouse flow
            else if ("FF_TO_CH".equals(direction)) {
                if ("uploadFile".equals(action)) {
                    if (file == null || file.isEmpty()) {
                        model.addAttribute("errorMessage", "Please select a file to upload");
                        return "index";
                    }
                    
                    FlatFileConfig fileConfig = new FlatFileConfig();
                    fileConfig.setFile(file);
                    fileConfig.setDelimiter(delimiter);
                    model.addAttribute("flatFileConfig", fileConfig);
                    
                    List<String> columns = flatFileService.parseFileHeaders(fileConfig);
                    model.addAttribute("columns", columns);
                } else if ("startIngestion".equals(action)) {
                    if (selectedColumns == null || selectedColumns.isEmpty()) {
                        model.addAttribute("errorMessage", "Please select at least one column");
                        return "index";
                    }
                    
                    ClickHouseConfig chConfig = new ClickHouseConfig();
                    chConfig.setHost(host);
                    chConfig.setPort(port);
                    chConfig.setDatabase(database);
                    chConfig.setUser(user);
                    chConfig.setPassword(password);
                    
                    FlatFileConfig fileConfig = new FlatFileConfig();
                    fileConfig.setDelimiter(delimiter);
                    
                    IngestionResult result = ingestionService.ingestFlatFileToClickHouse(fileConfig, chConfig, selectedColumns);
                    model.addAttribute("successMessage", "Ingestion completed: " + result.getRecordsProcessed() + 
                            " records processed. " + result.getMessage());
                }
            }
        } catch (Exception e) {
            model.addAttribute("errorMessage", "Error: " + e.getMessage());
        }
        
        return "index";
    }
} 