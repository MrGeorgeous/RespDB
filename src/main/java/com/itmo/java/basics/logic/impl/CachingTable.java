package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {

    protected Table table;
    protected DatabaseCache cache;

    public CachingTable(Table t) {
        this.table = t;
        this.cache = new DatabaseCacheImpl();
    }

    @Override
    public String getName() {
        return this.table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        this.cache.set(objectKey, objectValue);
        this.table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (this.cache.get(objectKey) != null) {
            return Optional.of(this.cache.get(objectKey));
        }
        return this.table.read(objectKey);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        this.cache.delete(objectKey);
        this.table.delete(objectKey);
    }
}
