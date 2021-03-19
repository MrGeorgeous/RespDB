package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.util.Optional;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if ((dbName == null) || (dbName.length() == 0)) {
            throw new DatabaseException("Empty db name.");
        }
        if (Files.exists(databaseRoot) && Files.isDirectory(databaseRoot)) {
            return new DatabaseImpl(dbName, databaseRoot);
        }
        throw new DatabaseException("Given path is not a directory");
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        this.validate(tableName);
        if (!tables.containsKey(tableName)) {
            tables.put(tableName, TableImpl.create(tableName, this.root, new TableIndex()));
        } else {
            throw new DatabaseException("Table already exists.");
        }
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        this.validate(tableName, objectKey, objectValue);
        if (tables.containsKey(tableName)) {
            tables.get(tableName).write(objectKey, objectValue);
        } else {
            throw new DatabaseException("No such table.");
        }
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        this.validate(tableName, objectKey);
        if (tables.containsKey(tableName)) {
            return tables.get(tableName).read(objectKey);
        } else {
            throw new DatabaseException("No such table.");
        }
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        this.validate(tableName, objectKey);
        if (tables.containsKey(tableName)) {
            tables.get(tableName).delete(objectKey);
        } else {
            throw new DatabaseException("No such table.");
        }
    }

    private DatabaseImpl(String dbName, Path databaseRoot) {
        this.name = dbName;
        this.root = databaseRoot;
        this.tables = new HashMap<>();
    }

    private String name;
    private Path root;
    private Map<String, Table> tables;

    private void validate(String tableName) throws DatabaseException {
        if ((tableName == null) || (tableName.length() == 0)) {
            throw new DatabaseException("Empty table name.");
        }
    }
    private void validate(String tableName, String objectKey) throws DatabaseException {
       this.validate(tableName);
        if ((objectKey == null) || (objectKey.length() == 0)) {
            throw new DatabaseException("Empty object key.");
        }
    }

    private void validate(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        this.validate(tableName, objectKey);
        if ((objectValue == null) || (objectValue.length == 0)) {
            throw new DatabaseException("Null object value.");
        }
    }



}
