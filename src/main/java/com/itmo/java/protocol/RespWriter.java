package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.io.OutputStream;

public class RespWriter implements AutoCloseable{

    private OutputStream stream;

    public RespWriter(OutputStream os) {
        this.stream = os;
    }

    /**
     * Записывает в output stream объект
     */
    public void write(RespObject object) throws IOException {
        object.write(stream);
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
