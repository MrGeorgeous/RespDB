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

    private byte[] data = null;

    public RespBulkString(byte[] data) {
        this.data = data;
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
        if (isEmpty()) {
            return null;
        } else {
            return new String(data);
        }
    }

    @Override
    public void write(OutputStream os) throws IOException {
        String r = (char)CODE + String.valueOf(NULL_STRING_SIZE) + "\r\n";
        if (!isEmpty()) {
            r = (char)CODE + String.valueOf(data.length) + "\r\n" + new String(data) + "\r\n";
        }
        os.write(r.getBytes());
    }

    private boolean isEmpty() {
        if (data == null) {
            return true;
        }
        if (data.length == 0) {
            return true;
        }
        return false;
    }

}
