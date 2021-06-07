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
    private PrintWriter requester = null;
    private BufferedReader responder = null;

    public SocketKvsConnection(ConnectionConfig config) {
        this.config = config;
        if (config.getPort() == 0) {
            throw new IllegalArgumentException("Empty port in configuration.");
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
            if ((this.socket == null) || (!this.socket.isConnected())) {
                //if (config.getPort() == null) {
                //    throw new ConnectionException("Empty port in configuration.");
                //}
                this.socket = new Socket(config.getHost(), config.getPort());
                requester = new PrintWriter(socket.getOutputStream(), true);
                responder = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            }
        } catch (Exception e) {
            throw new ConnectionException("Connection socket could not be opened.", e);
        }
        try {
            RespWriter rw = new RespWriter(socket.getOutputStream());
            rw.write(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

//            while(in.a)
//            String result = "";
//            String k;
//            while ((k = in.readLine()) != null) {
//                result += k + "\r\n";
//            }
//            RespReader rr = new RespReader(new ByteArrayInputStream(result.getBytes()));

            //while (socket.getInputStream().available() == 0);

            RespReader reader = new RespReader(socket.getInputStream());
            while (socket.isConnected()) {
                if (reader.hasObject()) {
                    RespObject r = reader.readObject();
                    if (r.isError()) {
                        throw new DatabaseExecutionException(r.asString(), null);
                    }
                    return r;
                }
            }

            throw new ConnectionException("Did not receive the response.", null);

        } catch (Exception e) {
            throw new ConnectionException("Processing request failed.", e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            if (socket != null) {
                socket.close();
            }
            if (requester != null) {
                requester.close();
            }
            if (responder != null) {
                responder.close();
            }
        } catch (Exception ignored) {}
    }


}
