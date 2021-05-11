package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {

    protected String databaseName;
    protected Path databaseRoot;
    protected Map<String, Table> tables;

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.databaseName = dbName;
        this.databaseRoot = databaseRoot;
        this.tables = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return this.databaseName;
    }

    @Override
    public Path getDatabasePath() {
        return this.databaseRoot.resolve(databaseName);
    }

    @Override
    public Map<String, Table> getTables() {
        return this.tables;
    }

    @Override
    public void addTable(Table table) {
        if (!this.tables.containsKey(table.getName())) {
            this.tables.put(table.getName(), table);
        }
    }

}
