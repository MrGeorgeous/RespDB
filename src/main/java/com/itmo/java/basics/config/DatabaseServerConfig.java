package com.itmo.java.basics.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@Builder
public class DatabaseServerConfig {
    private final ServerConfig serverConfig;

    private final DatabaseConfig dbConfig;

    public DatabaseServerConfig(ServerConfig serverConfig, DatabaseConfig dbConfig) {
        this.serverConfig = serverConfig;
        this.dbConfig = dbConfig;
    }

}
