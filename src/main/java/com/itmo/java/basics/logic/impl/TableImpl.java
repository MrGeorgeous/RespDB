package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;

public class TableImpl implements Table {

    private String tableName;
    private Path tablePath;
    private TableIndex tableIndex;
    private ArrayList<Segment> segments;

    public static Table initializeFromContext(TableInitializationContext context) {
        TableImpl t = new TableImpl(context.getTableName(), context.getTablePath(), context.getTableIndex());
        t.segments.add(context.getCurrentSegment());
        return new CachingTable(t);
    }

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {

        if ((tableName == null) || (tableName.length() == 0)) {
            throw new DatabaseException("tableName parameter is null or empty.");
        }

        if (!Files.isDirectory(pathToDatabaseRoot)) {
            throw new DatabaseException("Table is not given a valid path.");
        }

        Path tablePath = pathToDatabaseRoot.resolve(tableName);
        File f = new File(tablePath.toString());
        if (!Files.exists(tablePath) && !f.mkdir()) {
            throw new DatabaseException("Table directory can not be created.");
        }
        return new CachingTable(new TableImpl(tableName, tablePath, tableIndex));

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
                    throw new DatabaseException("Writing to new segment after overflow has failed unexpectedly.");
                }
            }
            tableIndex.onIndexedEntityUpdated(objectKey, segments.get(segments.size() - 1));
        } catch (IOException e) {
            throw new DatabaseException("Writing to segments failed due to the IO failure.", e);
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
                throw new DatabaseException("Reading the segment has failed due to the IO failure.", e);
            }
        }

        return Optional.empty();

    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        this.validateKey(objectKey);
        this.write(objectKey, null);
    }


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
        if ((objectKey == null) || (objectKey.length() == 0)) {
            throw new DatabaseException("objectKey parameter is null or empty.");
        }
    }

}
