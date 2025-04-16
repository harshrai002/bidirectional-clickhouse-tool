/**
 * FlatFileConfig.java Created by Harsh
 */
package com.zeotap.clickhouseingestion.dto;

import lombok.Getter;
import lombok.Setter;
import org.springframework.web.multipart.MultipartFile;

@Getter
@Setter
public class FlatFileConfig {
    private MultipartFile file;
    private String delimiter;
}
