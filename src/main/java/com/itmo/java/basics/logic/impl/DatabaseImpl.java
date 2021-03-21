package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
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
            throw new DatabaseException("Empty database name.");
        }

        Path dbPath = databaseRoot.resolve(dbName);
        File f = new File(dbPath.toString());
        if (!f.exists()) {
            if (!f.mkdir()) {
                throw new DatabaseException("DB directory can not be created.");
            }
        }

        if (Files.exists(dbPath) && Files.isDirectory(dbPath)) {
            return new DatabaseImpl(dbName, dbPath);
        }
        throw new DatabaseException("Given path is not a directory");

    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if ((tableName == null) || (tableName.length() == 0)) {
            throw new DatabaseException("Empty table name.");
        }
        if (this.tables.containsKey(tableName)) {
            throw new DatabaseException("Table already exists.");
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
        if (!this.tables.containsKey(tableName)){
            throw new DatabaseException("No such table.");
        }
    }

    private void validate(String tableName, String objectKey) throws DatabaseException {
        this.validate(tableName);
        if ((objectKey == null) || (objectKey.length() == 0)) {
            throw new DatabaseException("Empty object key.");
        }
    }


}
