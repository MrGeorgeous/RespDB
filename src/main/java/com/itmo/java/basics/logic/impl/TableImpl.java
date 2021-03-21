package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.util.Optional;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;

public class TableImpl implements Table {

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {

        if (tableName.length() == 0) {
            throw new DatabaseException("Empty table name.");
        }

        if (Files.isDirectory(pathToDatabaseRoot)) {
            Path tablePath = pathToDatabaseRoot.resolve(tableName);
            File f = new File(tablePath.toString());
            if (!Files.exists(tablePath)) {
                if (!f.mkdir()) {
                    throw new DatabaseException("Table directory can not be created.");
                }
            }
            return new TableImpl(tableName, tablePath, tableIndex);
        }

        throw new DatabaseException("Table is not given a valid path.");

    }

    @Override
    public String getName() {
        return this.tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {

        this.validateKey(objectKey);

        if (segments.size() == 0) {
            this.addNewSegment();
        }

        try {
            if (!segments.get(segments.size() - 1).write(objectKey, objectValue)) {
                this.addNewSegment();
                if (!segments.get(segments.size() - 1).write(objectKey, objectValue)) {
                    throw new DatabaseException("IO fault.");
                };
            }
            tableIndex.onIndexedEntityUpdated(objectKey, segments.get(segments.size() - 1));
        } catch (IOException e) {
            throw new DatabaseException("IO fault.");
        }

    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {

        this.validateKey(objectKey);

        Optional<Segment> s = this.tableIndex.searchForKey(objectKey);
        if (s.isPresent()) {
            try {
                return s.get().read(objectKey);
            } catch (IOException e) {
                throw new DatabaseException("IO fault.");
            }
        }

        return Optional.empty();

    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        this.validateKey(objectKey);
        this.write(objectKey, null);
    }

    private String tableName;
    private Path tablePath;
    private TableIndex tableIndex;
    private ArrayList<Segment> segments;

    private TableImpl(String tableName, Path tablePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.tablePath = tablePath;
        this.tableIndex = tableIndex;
        this.segments = new ArrayList<>();
    }

    private void addNewSegment() throws DatabaseException {
        segments.add(SegmentImpl.create(SegmentImpl.createSegmentName(this.tableName), this.tablePath));
    }

    private void validateKey(String objectKey) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("Empty object key.");
        }
        if (objectKey.length() == 0) {
            throw new DatabaseException("Empty object key.");
        }
    }

}
