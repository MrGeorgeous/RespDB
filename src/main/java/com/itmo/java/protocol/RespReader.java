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

    private Scanner scanner;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static String CRLF = String.valueOf((char)CR) + String.valueOf((char)LF);

    public RespReader(InputStream is) {
        this.scanner = new Scanner(is);
        this.scanner.useDelimiter("");
        System.out.println(CRLF);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return scanner.hasNext();
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {


        if (!scanner.hasNext()) {
            throw new EOFException("Empty InputStream.");
        }

        try {

//            System.out.println("lol");
//            String allt = new String(scanner.nextLine());
//            System.out.print(allt);
//            System.out.print("000");
            char code = readCode();

            if (code == RespCommandId.CODE) {
                int commandId = readInt();
                skipCRLF();
                //int commandId = ByteBuffer.wrap(stream.readNBytes(4)).getInt();
                return new RespCommandId(commandId);
            }

            if (code == RespError.CODE) {
                return new RespError(readUntilCRLF());
            }

            if (code == RespBulkString.CODE) {
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

            if (code == RespArray.CODE) {
                int len = readIntToCRLF();
                skipCRLF();
                ArrayList<RespObject> items = new ArrayList<>();
                for (int i = 0; i < len; i++) {
                    items.add(readObject());
                }
                return new RespArray(items.toArray(new RespObject[len]));
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
        System.out.println("a");
        //skipCRLF();
        System.out.println("b");
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
       scanner.close();
    }

    private char readCode() throws IOException {
        return (char) scanner.next().getBytes()[0];
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
            n[i] = scanner.next().getBytes()[0];
        }
        return n;
    }

    private void skipCRLF() throws IOException {
        for (int i = 0; i < CRLF.getBytes().length; i++) {
            scanner.next();
            //System.out.print(scanner.nextByte());
        }
    }

    private byte[] readUntilCRLF() throws IOException {
        byte t = scanner.next().getBytes()[0];
        List<Byte> bytes = new ArrayList<Byte>();
        while ((t != LF) && (scanner.hasNext())) {
            bytes.add(t);
            t = scanner.next().getBytes()[0];
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

}
