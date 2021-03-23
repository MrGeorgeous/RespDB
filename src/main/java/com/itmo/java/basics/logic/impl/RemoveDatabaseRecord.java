package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {

    private byte[] key;

    @Override
    public byte[] getKey() {
        return this.key;
    }

    @Override
    public byte[] getValue() {
        return null;
    }

    @Override
    public long size() {
        return 8 + this.key.length;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return this.key.length;
    }

    @Override
    public int getValueSize() {
        return -1;
    }

    public RemoveDatabaseRecord(byte[] k) {
        this.key = k;
    }

}
