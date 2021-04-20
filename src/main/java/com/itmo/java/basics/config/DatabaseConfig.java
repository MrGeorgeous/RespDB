package com.itmo.java.basics.config;

import java.nio.file.Paths;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";

    protected String workingPath;

    public DatabaseConfig(String workingPath) {
        this.workingPath = Paths.get(workingPath).resolve(DEFAULT_WORKING_PATH).toString();
    }

    public String getWorkingPath() {
        return this.workingPath;
    }
}
