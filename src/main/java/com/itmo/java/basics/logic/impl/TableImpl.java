package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import javax.xml.crypto.Data;
import java.nio.file.Path;
import java.util.Optional;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;

public class TableImpl implements Table {

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (Files.isDirectory(pathToDatabaseRoot)) {
            if (Files.isReadable(pathToDatabaseRoot) && Files.isWritable(pathToDatabaseRoot)) {
                Path tablePath = Paths.get(pathToDatabaseRoot.toString(), tableName, "\\");
                File f = new File(tablePath.toString());
                if (!Files.exists(tablePath)) {
                    if (!f.mkdir()) {
                        throw new DatabaseException("Table directory can not be created.");
                    }
                }
                return new TableImpl(tableName, tablePath, tableIndex);
            }
        }

        throw new DatabaseException("Table is not given a valid path.");

    }

    @Override
    public String getName() {
        return this.tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {

        this.validate(objectKey);

        if (segments.size() == 0) {
            this.addNewSegment();
        }
        if (!tryWrite(objectKey, objectValue)) {
            this.addNewSegment();
            if (!tryWrite(objectKey, objectValue)) {
                throw new DatabaseException("Impossible to write entry.");
            }
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {

        this.validate(objectKey);

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

        this.validate(objectKey);

        if (segments.size() == 0) {
            this.addNewSegment();
        }
        if (!tryDelete(objectKey)) {
            this.addNewSegment();
            if (!tryDelete(objectKey)) {
                throw new DatabaseException("Impossible to delete entry.");
            }
        }
    }

    private TableImpl(String tableName, Path tablePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.tablePath = tablePath;
        this.tableIndex = tableIndex;
        this.segments = new ArrayList<>();
    }

    protected String tableName;
    protected Path tablePath;
    protected TableIndex tableIndex;
    protected ArrayList<Segment> segments;

    protected void addNewSegment() throws DatabaseException {
        segments.add(SegmentImpl.create(SegmentImpl.createSegmentName(this.tableName), this.tablePath));
    }

    protected boolean tryWrite(String objectKey, byte[] objectValue) throws DatabaseException {
        try {
            if (segments.get(segments.size() - 1).write(objectKey, objectValue)) {
                tableIndex.onIndexedEntityUpdated(objectKey, segments.get(segments.size() - 1));
                return true;
            }
        } catch (IOException e) {
            throw new DatabaseException("IO fault.");
        }
        return false;
    }

    protected boolean tryDelete(String objectKey) throws DatabaseException {
        try {
            if (segments.get(segments.size() - 1).delete(objectKey)) {
                tableIndex.onIndexedEntityUpdated(objectKey, segments.get(segments.size() - 1));
                return true;
            }
        } catch (IOException e) {
            throw new DatabaseException("IO fault.");
        }
        return false;
    }

    private void validate(String objectKey) throws DatabaseException {
        if (objectKey.length() == 0) {
            throw new DatabaseException("Empty object key.");
        }
        if (objectKey.length() > 1000) {
            throw new DatabaseException("Too long object key.");
        }
    }

}
