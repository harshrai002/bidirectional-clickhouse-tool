/**
 * IngestionResult.java Created by Harsh
 */
package com.zeotap.clickhouseingestion.dto;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class IngestionResult {
    private long recordsProcessed;
    private String message;
}
