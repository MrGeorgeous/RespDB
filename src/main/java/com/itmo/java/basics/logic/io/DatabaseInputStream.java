package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.RemoveDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.nio.ByteBuffer;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {

    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {

        int keySize = this.readInt();
        byte[] key = this.readNBytes(keySize);
        int valueSize = this.readInt();

        if (valueSize != REMOVED_OBJECT_SIZE) {
            byte[] value = this.readNBytes(valueSize);
            return Optional.of(new SetDatabaseRecord(key, value));
        } else {
            return Optional.of(new RemoveDatabaseRecord(key));
        }

    }

}
