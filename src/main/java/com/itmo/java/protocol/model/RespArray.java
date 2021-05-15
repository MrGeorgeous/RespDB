package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    private List<RespObject> objects;

    public RespArray(RespObject... objects) {
        this.objects = new ArrayList<>();
        this.objects.addAll(Arrays.asList(objects));
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
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            String header = (char)CODE + String.valueOf(objects.size()) + "\r\n";
            stream.write(header.getBytes());
            for (RespObject obj : objects) {
                obj.write(stream);
            }
            return new String(stream.toByteArray());
        } catch (Exception e){
            // it must be impossible
            return (char)CODE + "0\r\n";
        }
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(asString().getBytes());
    }

    public List<RespObject> getObjects() {
        return this.objects;
    }

}
