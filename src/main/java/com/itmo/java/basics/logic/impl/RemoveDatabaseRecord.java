package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {

    @Override
    public byte[] getKey() {
        return this.key;
    }

    @Override
    public byte[] getValue() {
        return new byte[0];
    }

    @Override
    public long size() {
        long s = DatabaseInputStream.KEY_SIZE_FIELD + this.key.length + DatabaseInputStream.VALUE_SIZE_FIELD;
        return s;
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

    protected byte[] key;

}
