package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class RespReader implements AutoCloseable {
    private DataInputStream inner;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        this.inner = new DataInputStream(new BufferedInputStream(is));
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        byte[] b = new byte[1];
        inner.mark(1);
        int bytesRead = inner.read(b, 0, 1);
        inner.reset();

        return (bytesRead != -1) && (b[0] == RespArray.CODE);
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        inner.mark(1);
        byte code = inner.readByte();
        inner.reset();

        switch (code) {
            case RespError.CODE:
                return readError();
            case RespBulkString.CODE:
                return readBulkString();
            case RespArray.CODE:
                return readArray();
            case RespCommandId.CODE:
                return readCommandId();
            default:
                throw new IOException(String.format("RESP object code '%s' not recognized", code));
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        validateCode(RespError.CODE);
        byte[] message = readByteLine();

        return new RespError(message);
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        validateCode(RespBulkString.CODE);
        int len = readIntLiteral();

        if (len == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }

        byte[] data = readByteLineOfLen(len);

        return new RespBulkString(data);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        validateCode(RespArray.CODE);
        int len = readIntLiteral();
        RespObject[] objects = new RespObject[len];

        for (int i = 0; i < len; ++i) {
            try {
                objects[i] = readObject();
            } catch (EOFException e) {
                throw new IOException(String.format("Expected '%s' array elements, only received '%s'", len, i));
            }
        }

        return new RespArray(objects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        validateCode(RespCommandId.CODE);
        byte[] bytes = new byte[4];
        int bytesRead = inner.readNBytes(bytes, 0, 4);

        if (bytesRead != 4) {
            throw new IOException("Expected a four-byte integer");
        }

        validateCRLF();

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.order(ByteOrder.BIG_ENDIAN);
        int commandId = buf.getInt();

        return new RespCommandId(commandId);
    }


    @Override
    public void close() throws IOException {
        inner.close();
    }

    private void validateCode(byte expectedCode) throws IOException {
        byte code = inner.readByte();

        if (code != expectedCode) {
            throw new IOException(String.format("Expected RESP object code '%s', received '%s'", expectedCode, code));
        }
    }

    private byte[] readByteLine() throws IOException {
        List<Byte> data = new ArrayList<>();

        while (!(data.size() >= 2 && data.get(data.size() - 2) == CR && data.get(data.size() - 1) == LF)) {
            int inByte = inner.read();

            if (inByte == -1) {
                throw new IOException("Unexpected EOF when reading line");
            }

            data.add((byte) inByte);
        }

        data.remove(data.size() - 1);
        data.remove(data.size() - 1);

        byte[] res = new byte[data.size()];

        for (int i = 0; i < data.size(); ++i) {
            res[i] = data.get(i);
        }

        return res;
    }

    private int readIntLiteral() throws IOException {
        String literal = new String(readByteLine());

        try {
            return Integer.parseInt(literal);
        } catch (NumberFormatException e) {
            throw new IOException(String.format("Expected valid integer literal, received '%s'", literal), e);
        }
    }

    private byte[] readByteLineOfLen(int len) throws IOException {
        byte[] data = new byte[len];
        int bytesRead = inner.readNBytes(data, 0, len);

        if (bytesRead != len) {
            throw new IOException(String.format("Expected line of length '%s', only read '%s'", len, bytesRead));
        }

        validateCRLF();

        return data;
    }

    private void validateCRLF() throws IOException {
        if (inner.read() != CR || inner.read() != LF) {
            throw new IOException("Expected CRLF");
        }
    }
}
