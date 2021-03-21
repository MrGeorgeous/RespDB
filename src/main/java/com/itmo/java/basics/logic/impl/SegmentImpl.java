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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import java.io.*;
import java.nio.file.Files;


public class SegmentImpl implements Segment {

    private static final int MAX_SEGMENT_SIZE = 100_000; // in bytes

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {

        //if (segmentName.length() == 0) {
        //    throw new DatabaseException("Empty segment name.");
        //}

        //if (Files.isDirectory(tableRootPath)) {
        //    if (Files.isReadable(tableRootPath) && Files.isWritable(tableRootPath)) {

                File segmentFile = new File(new File(tableRootPath.toString()), segmentName);
                segmentFile.getParentFile().mkdirs();

                if (!(segmentFile.exists())) {
                    try {
                        Files.createFile(segmentFile.toPath());
                    } catch (IOException e) {
                        //throw new DatabaseException("Segment file can not be created.");
                    }
                }

                try {
                    OutputStream ioStream = new FileOutputStream(segmentFile.toPath().toAbsolutePath().toString(), true);
                    DatabaseOutputStream outDbStream = new DatabaseOutputStream(ioStream);
                    return new SegmentImpl(segmentName, segmentFile.toPath().toAbsolutePath(), outDbStream);
                } catch (Exception e) {

                }


        //    }
        //}

        throw new DatabaseException("Path is not valid.");

    }

    public static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return this.segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {

        if (this.isReadOnly() /*|| (this.getOffset() + objectValue.length > MAX_SEGMENT_SIZE)*/) {
            return false;
        }

        WritableDatabaseRecord record;
        if (objectValue != null) {
            record = new SetDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8), objectValue);
        } else {
            record = new RemoveDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8));
        }

        long offset = this.getOffset();

        this.currentOffset += outDbStream.write(record);
        this.segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(offset));
        outDbStream.flush();

        if (this.isReadOnly()) {
            this.outDbStream.close();
        }

        return true;

    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {

        InputStream ioStream = new FileInputStream(this.segmentPath.toString());
        DatabaseInputStream dbStream = new DatabaseInputStream(ioStream);

        DatabaseRecord record = null;

        Optional<SegmentOffsetInfo> offsetInfo = segmentIndex.searchForKey(objectKey);
        if (offsetInfo.isPresent()) {
            int offset = (int) offsetInfo.get().getOffset();
            if (dbStream.skip(offset) == offset) {
                Optional<DatabaseRecord> r = dbStream.readDbUnit();
                if (r.isPresent()) {
                    record = r.get();
                }
            }
        }

        if ((record != null) && record.isValuePresented() /* && (record.getValue().length != 0) */) {
            return Optional.of(record.getValue());
        }

        return Optional.empty();

    }

    @Override
    public boolean isReadOnly() {
        return (getOffset() >= MAX_SEGMENT_SIZE);
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        return this.write(objectKey, null);
    }

    private String segmentName;
    private Path segmentPath;
    private SegmentIndex segmentIndex;
    private long currentOffset;
    private DatabaseOutputStream outDbStream = null;

    private SegmentImpl(String _segmentName, Path _segmentPath, DatabaseOutputStream _outDbStream) {
        this.segmentName = _segmentName;
        this.segmentPath = _segmentPath;
        this.segmentIndex = new SegmentIndex();
        this.currentOffset = 0;
        this.outDbStream = _outDbStream;
    }

    private long getOffset() {
        return this.currentOffset;
        //File f = new File(this.segmentPath.toString());
        //return f.length();
    }

}
