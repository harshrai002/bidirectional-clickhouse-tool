/**
 * ClickHouseConfig.java Created by Harsh
 */
package com.zeotap.clickhouseingestion.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ClickHouseConfig {
    private String host;
    private int port;
    private String database;
    private String user;
    private String password;
    private String protocol;
}
