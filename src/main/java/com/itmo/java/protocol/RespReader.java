package com.itmo.java.protocol;

import com.itmo.java.protocol.model.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

public class RespReader implements AutoCloseable {

    private InputStream stream;
    private byte buffer = 0;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static String CRLF = String.valueOf((char)CR) + String.valueOf((char)LF);

    public RespReader(InputStream is) {
        this.stream = new DataInputStream(new BufferedInputStream(is));
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        if (end()) {
            return false;
        }
        return getNextByte() == RespArray.CODE;
    }

    public boolean hasObject() {
        return !end();
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        try {
            ensureNotEnd();
            char code = (char)getNextByte();
            switch (code) {
                case RespCommandId.CODE:
                    return readCommandId();
                case RespError.CODE:
                    return readError();
                case RespBulkString.CODE:
                    return readBulkString();
                case RespArray.CODE:
                    return readArray();
                default:
                    throw new IOException("Unknown RespObject was found.");
            }
        } catch (IOException e) {
            throw new IOException("RespObject was found corrupted while reading from stream.", e);
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
        return new RespError(readUntilCRLF());
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        validateCode(RespBulkString.CODE);
        int len = readIntToCRLF();
        if (len == RespBulkString.NULL_STRING_SIZE) {
            return new RespBulkString(null);
        }
        byte[] str = readNBytes(len);
        skipCRLF();
        if (str.length != len) {
            throw new IOException("Corrupted bulk string.");
        }
        return new RespBulkString(str);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        validateCode(RespArray.CODE);
        int len = readIntToCRLF();
        ArrayList<RespObject> items = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            items.add(readObject());
        }
        return new RespArray(items.toArray(new RespObject[len]));
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        validateCode(RespCommandId.CODE);
        int commandId = ByteBuffer.wrap(readNBytes(4)).getInt();
        skipCRLF();
        return new RespCommandId(commandId);
    }


    @Override
    public void close() throws IOException {
       stream.close();
    }

    private void validateCode(byte t) throws IOException {
        ensureNotEnd();
        if (readNextByte() != t) {
            throw new IOException("Unexpected RESP object.");
        }
    }

    private byte[] readNBytes(int N) throws IOException {
        byte[] n = new byte[N];
        for (int i = 0; i < N; i++) {
            n[i] = readNextByte();
        }
        return n;
    }

    private void skipCRLF() throws IOException {
        String read = "";
        for (int i = 0; i < CRLF.getBytes().length; i++) {
           read += (char)readNextByte();
        }
        if (!read.equals(CRLF)) {
            String s = new String(readUntilCRLF());
            if (!s.equals("")) {
                throw new IOException("Wrong CRLF skip.");
            }
        }
    }

    private byte[] readUntilCRLF() throws IOException {
        byte t = readNextByte();
        List<Byte> bytes = new ArrayList<Byte>();
        while (t != LF) {
            bytes.add(t);
            t = readNextByte();
        }
        if ((bytes.size() >= 2) && (bytes.get(bytes.size() - 1) != CR)) {
            throw new IOException("Wrong CRLF skip.");
        }
        byte[] b = new byte[bytes.size() - 1];
        for (int i = 0; i < bytes.size() - 1; i++) {
            b[i] = bytes.get(i);
        }
        return b;
    }

    private int readIntToCRLF() throws IOException {
        return Integer.parseInt(new String(readUntilCRLF()));
    }

    private byte getNextByte() throws IOException {
        if (buffer == 0) {
            buffer = readNextByte(true);
        }
        return buffer;
    }

    private byte readNextByte() throws IOException {
        return readNextByte(false);
    }

    private byte readNextByte(boolean suspendEOF) throws IOException {
        if (buffer != 0) {
            byte r = buffer;
            buffer = 0;
            return r;
        }

        byte[] b = new byte[1];
        int bytesRead = stream.read(b, 0, 1);

        if (bytesRead != 1) {
            buffer = -1;
            throw new EOFException("End of stream.");
        }

        return b[0];
    }

    private boolean end() {
        try {
            return getNextByte() == -1;
        } catch (Exception e) {
            return true;
        }
    }

    private void ensureNotEnd() throws EOFException {
        if (end()) {
            throw new EOFException("Empty InputStream.");
        }
    }

}
