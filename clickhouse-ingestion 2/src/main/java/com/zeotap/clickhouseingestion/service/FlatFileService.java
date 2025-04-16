package com.zeotap.clickhouseingestion.service;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.zeotap.clickhouseingestion.dto.FlatFileConfig;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class FlatFileService {

    private File tempFile;
    private List<String> headers;

    public List<String> parseFileHeaders(FlatFileConfig config) throws Exception {
        MultipartFile file = config.getFile();
        char delimiter = config.getDelimiter().charAt(0);

        // Save the uploaded file to a temporary location
        tempFile = File.createTempFile("upload-", ".csv");
        file.transferTo(tempFile);

        CSVParser parser = new CSVParserBuilder()
                .withSeparator(delimiter)
                .build();

        try (Reader reader = new InputStreamReader(Files.newInputStream(tempFile.toPath()));
             CSVReader csvReader = new CSVReaderBuilder(reader)
                     .withCSVParser(parser)
                     .build()) {

            String[] headerRow = csvReader.readNext();
            if (headerRow != null) {
                headers = Arrays.asList(headerRow);
                return headers;
            }
        }

        return new ArrayList<>();
    }
    public List<List<String>> readData(FlatFileConfig config, List<String> selectedColumns) throws Exception {
        if (tempFile == null || !tempFile.exists()) {
            throw new Exception("No file has been uploaded");
        }
        
        char delimiter = config.getDelimiter().charAt(0);
        List<List<String>> data = new ArrayList<>();
        List<Integer> columnIndices = new ArrayList<>();
        
        // Find indices of selected columns
        for (String column : selectedColumns) {
            int index = headers.indexOf(column);
            if (index >= 0) {
                columnIndices.add(index);
            }
        }
        
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(delimiter)
                .build();
        
        try (Reader reader = new InputStreamReader(Files.newInputStream(tempFile.toPath()));
             CSVReader csvReader = new CSVReaderBuilder(reader)
                     .withCSVParser(parser)
                     .withSkipLines(1)  // Skip header row
                     .build()) {
            
            String[] row;
            while ((row = csvReader.readNext()) != null) {
                List<String> selectedData = new ArrayList<>();
                for (int index : columnIndices) {
                    if (index < row.length) {
                        selectedData.add(row[index]);
                    } else {
                        selectedData.add("");  // Handle missing values
                    }
                }
                data.add(selectedData);
            }
        }
        
        return data;
    }
    
    public File writeToFile(List<String> headers, List<List<String>> data, char delimiter) throws Exception {
        File outputFile = File.createTempFile("export-", ".csv");
        
        try (CSVWriter writer = new CSVWriter(new FileWriter(outputFile),
                delimiter,
                CSVWriter.DEFAULT_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)) {
            
            // Write headers
            writer.writeNext(headers.toArray(new String[0]));
            
            // Write data
            for (List<String> row : data) {
                writer.writeNext(row.toArray(new String[0]));
            }
        }
        
        return outputFile;
    }
    
    public void cleanup() {
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }
} 