package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {

    protected final int CACHE_SIZE = 30;
    protected Map<String, byte[]> pairs;

    public DatabaseCacheImpl() {
        pairs = new LinkedHashMap<>(CACHE_SIZE, 0.8f, true);
    }

    @Override
    public byte[] get(String key) {
        if (this.pairs.containsKey(key)) {
            return this.pairs.get(key);
        }
        return null;
    }

    @Override
    public void set(String key, byte[] value) {
        if (!this.pairs.containsKey(key) && (this.pairs.size() == CACHE_SIZE)) {
            Iterator<String> it = this.pairs.keySet().iterator();
            it.next();
            it.remove();
        }
        this.pairs.put(key, value);
    }

    @Override
    public void delete(String key) {
        this.pairs.remove(key);
    }
}
