package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.io.File;
import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

    private String segmentName;
    private Path segmentPath;
    private SegmentIndex segmentIndex;
    private int currentSize;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.segmentIndex = index;
        this.currentSize = currentSize;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this.segmentName = segmentName;
        this.segmentPath = tablePath.resolve(segmentName);
        this.segmentIndex = new SegmentIndex();
        this.currentSize = currentSize;
    }

    @Override
    public String getSegmentName() {
        return this.segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return this.segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return this.segmentIndex;
    }

    @Override
    public long getCurrentSize() {
        return this.currentSize;
    }
}
