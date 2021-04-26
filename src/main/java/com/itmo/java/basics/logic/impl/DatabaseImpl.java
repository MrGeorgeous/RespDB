package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import javax.xml.crypto.Data;
import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class DatabaseImpl implements Database {

    private String name;
    private Path root;
    private Map<String, Table> tables;

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        DatabaseImpl d = new DatabaseImpl(context.getDbName(), context.getDatabasePath().getParent());
        for (Table t : context.getTables().values()) {
            d.tables.put(t.getName(), t);
        }
        return d;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {

        if ((dbName == null) || (dbName.length() == 0)) {
            throw new DatabaseException("dbName parameter is null or an empty string.");
        }

        Path dbPath = databaseRoot.resolve(dbName);
        File f = new File(dbPath.toString());
        if (!f.exists() && !f.mkdir()) {
            throw new DatabaseException("Database subdirectory could not be accessed or created.");
        }

        return new DatabaseImpl(dbName, dbPath);

    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if ((tableName == null) || (tableName.length() == 0)) {
            throw new DatabaseException("tableName parameter is null or empty.");
        }
        if (this.tables.containsKey(tableName)) {
            throw new DatabaseException("Table '" + tableName + "' already exists and can not be created.");
        }
        this.tables.put(tableName, TableImpl.create(tableName, this.root, new TableIndex()));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        this.validate(tableName, objectKey);
        this.tables.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        this.validate(tableName, objectKey);
        return this.tables.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        this.validate(tableName, objectKey);
        this.tables.get(tableName).delete(objectKey);
    }

    public DatabaseImpl(String dbName, Path databaseRoot) {
        this.name = dbName;
        this.root = databaseRoot;
        this.tables = new HashMap<>();
    }

    private void validate(String tableName) throws DatabaseException {
        if ((tableName == null) || (tableName.length() == 0)) {
            throw new DatabaseException("tableName parameter is null or empty.");
        }
        if (!this.tables.containsKey(tableName)){
            throw new DatabaseException("There is no table corresponding to '" + tableName + "' tableName parameter.");
        }
    }

    private void validate(String tableName, String objectKey) throws DatabaseException {
        this.validate(tableName);
        if ((objectKey == null) || (objectKey.length() == 0)) {
            throw new DatabaseException("objectKey parameter is null or an empty string.");
        }
    }

}
