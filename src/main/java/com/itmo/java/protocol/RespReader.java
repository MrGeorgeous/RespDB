package com.itmo.java.protocol;

import com.itmo.java.protocol.model.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
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
        this.stream = is;
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        if (end()) {
            return false;
        }
        byte r = getNextByte();
        return r == RespArray.CODE;
    }

    public boolean hasObject() throws IOException {
        if (end()) {
            return false;
        }
        return true;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {

        if (end()) {
            throw new EOFException("Empty InputStream.");
        }

        try {

            char code = (char)getNextByte();

            if (code == RespCommandId.CODE) {
                return readCommandId();
            }

            if (code == RespError.CODE) {
               return readError();
            }

            if (code == RespBulkString.CODE) {
               return readBulkString();
            }

            if (code == RespArray.CODE) {
               return readArray();
            }

        } catch (IOException e) {
            throw new IOException("RespObject was found corrupted while reading from stream.", e);
        }

        throw new IOException("Unknown RespObject was found.");

    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        char code = readCode();
        return new RespError(readUntilCRLF());
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        char code = readCode();
        int len = readIntToCRLF();
        if (len == -1) {
            return new RespBulkString(null);
        }
        byte[] str = readNBytes(len);
        skipCRLF();
        assert(str.length == len);
        //byte[] str = stream.readNBytes(len);
        return new RespBulkString(str);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        char code = readCode();
        int len = readIntToCRLF();
        //skipCRLF();
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
        char code = readCode();
        int commandId = readInt();
        skipCRLF();
        //int commandId = ByteBuffer.wrap(stream.readNBytes(4)).getInt();
        return new RespCommandId(commandId);
    }


    @Override
    public void close() throws IOException {
       stream.close();
    }

    private char readCode() throws IOException {
        return (char)readNextByte();
//        char r = scanner.next(".").charAt(0);
//        scanner.useDelimiter("");
//        return r;
    }

    private int readInt() throws IOException {
        return ByteBuffer.wrap(readNBytes(4)).getInt();
    }

    private byte[] readNBytes(int N) throws IOException {
        byte[] n = new byte[N];
        for (int i = 0; i < N; i++) {
            n[i] = readNextByte();
        }
        return n;
    }

    private void skipCRLF() throws IOException {
        for (int i = 0; i < CRLF.getBytes().length; i++) {
           System.out.print((char)readNextByte());
            //System.out.print(scanner.nextByte());
        }
    }

    private byte[] readUntilCRLF() throws IOException {
        byte t = readNextByte();
        List<Byte> bytes = new ArrayList<Byte>();
        while ((t != LF) && !end()) {
            bytes.add(t);
            t = readNextByte();
        }
        byte[] b = new byte[bytes.size() - 1];
        for (int i = 0; i < bytes.size() - 1; i++) {
            b[i] = bytes.get(i);
        }
        return b;
    }

    private int readIntToCRLF() throws IOException {
        return Integer.parseInt(new String(readUntilCRLF()));
        //return ByteBuffer.wrap(readUntilCRLF()).getInt();
    }

    private byte getNextByte() throws IOException {
        if (buffer == 0) {
            buffer = readNextByte();
        }
        return buffer;
    }

    private byte readNextByte() throws IOException {
        if (buffer != 0) {
            byte r = buffer;
            buffer = 0;
            return r;
        }
        byte r = (byte) stream.read();
        //byte r = stream.readNBytes(1)[0];
        return r;
    }

    private boolean end() throws IOException {
        return getNextByte() == -1;
        //return stream.available() == 0;
    }




}
