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
        return 8 + this.key.length + this.value.length;
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

    private byte[] key;
    private byte[] value;

}
