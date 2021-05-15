package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    /**
     * Код объекта
     */
    public static final byte CODE = '$';

    public static final int NULL_STRING_SIZE = -1;

    private byte[] data;

    public RespBulkString(byte[] data) {
        if (data == null) {
            this.data = new byte[0];
        } else {
            this.data = data;
        }
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
       return new String(data);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        String r = (char)CODE + String.valueOf(NULL_STRING_SIZE) + "\r\n";
        if (data.length != 0) {
            r = (char)CODE + String.valueOf(data.length) + "\r\n" + new String(data) + "\r\n";
        }
        os.write(r.getBytes());
    }
}
