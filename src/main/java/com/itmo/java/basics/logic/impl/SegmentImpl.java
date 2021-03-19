package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class SegmentImpl implements Segment {

    private static final int MAX_SEGMENT_SIZE = 100_000; // in bytes

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {

        if (segmentName.length() == 0) {
            throw new DatabaseException("Empty segment name.");
        }

        if (Files.isDirectory(tableRootPath)) {
            if (Files.isReadable(tableRootPath) && Files.isWritable(tableRootPath)) {
                Path segmentPath = tableRootPath.resolve(segmentName);
                if (!Files.exists(segmentPath)) {
                    try {
                        File segmentFile = new File(segmentPath.toString());
                        //System.out.println(segmentPath.toString());
                        if (segmentFile.createNewFile()) {
                            return new SegmentImpl(segmentName, segmentPath);
                        }
                    } catch (Exception e) {

                    }
                    throw new DatabaseException("Segment file can not be created.");
                }
            }
        }

        throw new DatabaseException("Path is not valid.");

    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return this.segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {

        if (objectKey.getBytes().length + objectValue.length > MAX_SEGMENT_SIZE) {
            return false;
            //throw new DatabaseException("Too long entry.");
        }

        WritableDatabaseRecord record = new SetDatabaseRecord(objectKey.getBytes(), objectValue);
        if (!isReadOnly() && (getOffset() <= MAX_SEGMENT_SIZE)) {
            OutputStream ioStream = new FileOutputStream(this.segmentPath.toString(), true);
            DatabaseOutputStream stream = new DatabaseOutputStream(ioStream);
            long offset = this.getOffset();
            if (stream.write(record) == record.size()) {
                this.segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(offset));
                return true;
            }
        }

        return false;

    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {

        InputStream ioStream = new FileInputStream(this.segmentPath.toString());
        DatabaseInputStream dbStream = new DatabaseInputStream(ioStream);

        DatabaseRecord record = null;
        Optional<SegmentOffsetInfo> offset = segmentIndex.searchForKey(objectKey);
        if (offset.isPresent()) {
            dbStream.skip(offset.get().getOffset());
            Optional<DatabaseRecord> r = dbStream.readDbUnit();
            if (r.isPresent()) {
                record = r.get();
            }
        }

        if ((record != null) && (record.isValuePresented())) {
            if (record.getValue().length != 0) {
                return Optional.of(record.getValue());
            }
        }

        return Optional.empty();

    }

    @Override
    public boolean isReadOnly() {
        File segmentFile = new File(this.segmentPath.toString());
        return segmentFile.length() > MAX_SEGMENT_SIZE;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {

        WritableDatabaseRecord record = new RemoveDatabaseRecord(objectKey.getBytes());
        if (!isReadOnly() && (getOffset() + record.size() <= MAX_SEGMENT_SIZE)) {
            OutputStream ioStream = new FileOutputStream(this.segmentPath.toString(), true);
            DatabaseOutputStream stream = new DatabaseOutputStream(ioStream);
            long offset = this.getOffset();
            if (stream.write(record) == record.size()) {
                this.segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(offset));
                return true;
            }
        }

        return false;

    }

    private SegmentImpl(String _segmentName, Path _segmentPath) {
        this.segmentName = _segmentName;
        this.segmentPath = _segmentPath;
        this.segmentIndex = new SegmentIndex();
    }

    private String segmentName;
    private Path segmentPath;
    private SegmentIndex segmentIndex;



    private long getOffset() {
        File f = new File(this.segmentPath.toString());
        return f.length();
    }

}
