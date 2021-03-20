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

        if (tableName.length() == 0) {
            throw new DatabaseException("Empty segment name.");
        }

        if (Files.isDirectory(pathToDatabaseRoot)) {
            if (Files.isReadable(pathToDatabaseRoot) && Files.isWritable(pathToDatabaseRoot)) {
                Path tablePath = pathToDatabaseRoot.resolve(tableName);
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

            try {
                if (!segments.get(segments.size() - 1).write(objectKey, objectValue)) {
                    this.addNewSegment();
                    segments.get(segments.size() - 1).write(objectKey, objectValue);
                }
            } catch (IOException e) {
                //throw new DatabaseException("IO fault.");
            }

        tableIndex.onIndexedEntityUpdated(objectKey, segments.get(segments.size() - 1));


        //if (!tryWrite(objectKey, objectValue)) {
        //    throw new DatabaseException("Impossible to write entry.");
//            this.addNewSegment();
//            if (!tryWrite(objectKey, objectValue)) {
//                this.rollbackNewSegment();
//                throw new DatabaseException("Impossible to write entry.");
//            }
        //}

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
        this.write(objectKey, null);

//        if (segments.size() == 0) {
//            this.addNewSegment();
//        }
//        if (!tryDelete(objectKey)) {
//            this.addNewSegment();
//            if (!tryDelete(objectKey)) {
//                throw new DatabaseException("Impossible to delete entry.");
//            }
//        }

    }

    protected String tableName;
    protected Path tablePath;
    protected TableIndex tableIndex;
    protected ArrayList<Segment> segments;

    private TableImpl(String tableName, Path tablePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.tablePath = tablePath;
        this.tableIndex = tableIndex;
        this.segments = new ArrayList<>();
    }

    protected void addNewSegment() throws DatabaseException {
        segments.add(SegmentImpl.create(SegmentImpl.createSegmentName(this.tableName), this.tablePath));
    }

    protected void rollbackNewSegment() throws DatabaseException {
        segments.remove(segments.size() - 1);
    }

//    protected boolean tryDelete(String objectKey) throws DatabaseException {
//        return this.tryWrite(objectKey, null);
////        try {
////            if (segments.get(segments.size() - 1).delete(objectKey)) {
////                tableIndex.onIndexedEntityUpdated(objectKey, segments.get(segments.size() - 1));
////                return true;
////            }
////        } catch (IOException e) {
////            throw new DatabaseException("IO fault.");
////        }
////        return false;
//    }

    private void validate(String objectKey) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("Empty object key.");
        }
        if (objectKey.length() == 0) {
            throw new DatabaseException("Empty object key.");
        }
//        if (objectKey.length() > 1000) {
//            throw new DatabaseException("Too long object key.");
//        }
    }

}
