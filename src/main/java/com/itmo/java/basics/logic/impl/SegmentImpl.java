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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import java.nio.file.Files;


public class SegmentImpl implements Segment {

    private static final int MAX_SEGMENT_SIZE = 100_000;

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
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {

        if ((segmentName == null) || (segmentName.length() == 0)) {
            throw new DatabaseException("segmentName parameter is null or empty.");
        }

        if (!Files.isDirectory(tableRootPath)) {
            throw new DatabaseException("tableRootPath parameter does not present an existing directory.");
        }

        File segmentFile = new File(new File(tableRootPath.toString()), segmentName);
        try {
            if (!segmentFile.exists()) {
                Files.createFile(segmentFile.toPath());
            }
            OutputStream ioStream = new FileOutputStream(segmentFile.toPath().toAbsolutePath().toString(), true);
            DatabaseOutputStream outDbStream = new DatabaseOutputStream(ioStream);
            return new SegmentImpl(segmentName, segmentFile.toPath().toAbsolutePath(), outDbStream);
        } catch (IOException e) {
            throw new DatabaseException("Segment file could not be created or accessed.", e);
        }

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

        if (this.isReadOnly()) {
            outDbStream.flush();
            outDbStream.close();
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

        return true;

    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {

        Optional<SegmentOffsetInfo> offsetInfo = segmentIndex.searchForKey(objectKey);
        if (offsetInfo.isEmpty()) { return Optional.empty(); }
        int offset = (int) offsetInfo.get().getOffset();

        InputStream ioStream = new FileInputStream(this.segmentPath.toString());
        DatabaseInputStream dbStream = new DatabaseInputStream(ioStream);

        if (dbStream.skip(offset) != offset) {
            throw new IOException("Entry has been found corrupted due to invalid index offset.");
        }

        Optional<DatabaseRecord> record = dbStream.readDbUnit();
        if (record.isPresent() && record.get().isValuePresented()) {
            return Optional.of(record.get().getValue());
        }

        return Optional.empty();

    }

    @Override
    public boolean isReadOnly() {
        return getOffset() >= MAX_SEGMENT_SIZE;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        return this.write(objectKey, null);
    }

}
