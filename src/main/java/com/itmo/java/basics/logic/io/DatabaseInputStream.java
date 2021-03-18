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

    // THESE VALUES ARE HARDCODED
    // AND ARE NOT SUBJECT TO CHANGES
    public static final int KEY_SIZE_FIELD = 4; // in bytes (int)
    public static final int VALUE_SIZE_FIELD = 4; // in bytes (int)
    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {

        Optional<byte[]> buffer;

        // read key size
        buffer = this.tryReadNBytes(KEY_SIZE_FIELD);
        if (buffer.isEmpty()) {
            return Optional.empty();
        }
        int keySize = ByteBuffer.wrap(buffer.get()).getInt();

        // read key
        buffer = this.tryReadNBytes(keySize);
        if (buffer.isEmpty()) {
            return Optional.empty();
        }
        byte[] key = buffer.get();

        // read value size
        buffer = this.tryReadNBytes(VALUE_SIZE_FIELD);
        if (buffer.isEmpty()) {
            return Optional.empty();
        }
        int valueSize = ByteBuffer.wrap(buffer.get()).getInt();

        // read value
        if (valueSize != REMOVED_OBJECT_SIZE) {
            Optional<byte[]> value = this.tryReadNBytes(valueSize);
            if (value.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new SetDatabaseRecord(key, value.get()));
        } else {
            return Optional.of(new RemoveDatabaseRecord(key));
        }

    }

    private Optional<byte[]> tryReadNBytes(int N) throws IOException {
        byte[] data = this.readNBytes(N);
        if (data.length == N) {
            return Optional.of(data);
        }
        return Optional.empty();
    }

}
