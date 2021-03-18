package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

public class SetDatabaseRecord implements WritableDatabaseRecord {

    @Override
    public byte[] getKey() {
        return this.key;
    }

    @Override
    public byte[] getValue() {
        return this.value;
    }

    @Override
    public long size() {
        long s = DatabaseInputStream.KEY_SIZE_FIELD + this.key.length + DatabaseInputStream.VALUE_SIZE_FIELD;
        s += this.value.length;
        return s;
    }

    @Override
    public boolean isValuePresented() {
        return true;
    }

    @Override
    public int getKeySize() {
        return this.key.length;
    }

    @Override
    public int getValueSize() {
        return this.value.length;
    }

    public SetDatabaseRecord(byte[] k, byte[] v) {
        this.key = k;
        this.value = v;
    }

    protected byte[] key;
    protected byte[] value;

}
