package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.util.stream.Collectors;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {

    private ConnectionConfig config;
    private Socket socket = null;
    private RespWriter writer = null;
    private RespReader reader = null;

    public SocketKvsConnection(ConnectionConfig config) {
        this.config = config;
        try {
            this.socket = new Socket(config.getHost(), config.getPort());
            this.writer = new RespWriter(socket.getOutputStream());
            this.reader = new RespReader(socket.getInputStream());
        } catch (Exception e) {
            throw new IllegalArgumentException("send: Connection socket could not be opened. ", e);
        }

    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {

        try {
            writer.write(command);
        } catch (IOException e) {
            throw new ConnectionException("Failed to send request.", e);
        }

        try {
            return reader.readObject();
        } catch (IOException e) {
            throw new ConnectionException("Failed to get response.", e);
        }

    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (Exception ignored) {
        }
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (Exception ignored) {
        }
        try {
            socket.close();
        } catch (Exception ignored) {
        }
    }


}
