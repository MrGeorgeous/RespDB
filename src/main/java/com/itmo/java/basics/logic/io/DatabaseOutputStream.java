package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Записывает данные в БД
 */
public class DatabaseOutputStream extends DataOutputStream {

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    /**
     * Записывает в БД в следующем формате:
     * - Размер ключа в байтахб используя {@link WritableDatabaseRecord#getKeySize()}
     * - Ключ
     * - Размер записи в байтах {@link WritableDatabaseRecord#getValueSize()}
     * - Запись
     * Например при использовании UTF_8,
     * "key" : "value"
     * 3key5value
     * Метод вернет 10
     *
     * @param databaseRecord запись
     * @return размер записи
     * @throws IOException если запись не удалась
     */
    public int write(WritableDatabaseRecord databaseRecord) throws IOException {
        this.writeInt(databaseRecord.getKeySize());
        this.write(databaseRecord.getKey());
        this.writeInt(databaseRecord.getValueSize());
        if (databaseRecord.isValuePresented()) {
            this.write(databaseRecord.getValue());
        }
        return (int) databaseRecord.size();
    }

}