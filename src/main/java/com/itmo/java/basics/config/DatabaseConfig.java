package com.itmo.java.basics.config;

import java.nio.file.Path;
import java.nio.file.Paths;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";

    protected Path workingPath;

    public DatabaseConfig(String workingPath) {
        this.workingPath = Paths.get(workingPath).toAbsolutePath().resolve(DEFAULT_WORKING_PATH);
    }

    public String getWorkingPath() {
        return this.workingPath.toString();
    }
}
