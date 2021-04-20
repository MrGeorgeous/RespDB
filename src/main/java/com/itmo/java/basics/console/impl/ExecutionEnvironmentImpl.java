package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {

    private Map<String, Database> databases;
    private DatabaseConfig configuration;

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.databases = new HashMap<>();
        this.configuration = config;
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        if (!this.databases.containsKey(name)) {
            return Optional.empty();
        }
        return Optional.of(databases.get(name));
    }

    @Override
    public void addDatabase(Database db) {
        if (!this.databases.containsKey(db.getName())) {
            this.databases.put(db.getName(), db);
        }
    }

    @Override
    public Path getWorkingPath() {
        return Paths.get(this.configuration.getWorkingPath()).toAbsolutePath();
    }



}
