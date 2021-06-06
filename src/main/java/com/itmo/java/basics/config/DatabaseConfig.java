package com.itmo.java.basics.config;

import java.nio.file.Path;
import java.nio.file.Paths;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";

    protected String workingPath;

    public DatabaseConfig() {
        this.workingPath = DEFAULT_WORKING_PATH;
        //this.workingPath = Paths.get("").toAbsolutePath().resolve(DEFAULT_WORKING_PATH).toString();
    }

    public DatabaseConfig(String workingPath) {
        this.workingPath = workingPath;
    }

    public String getWorkingPath() {
        return this.workingPath;
    }
}
