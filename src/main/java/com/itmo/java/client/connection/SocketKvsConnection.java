package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.net.Socket;
import java.util.stream.Collectors;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {

    private ConnectionConfig config;
    private Socket socket;
    private PrintWriter requester = null;
    private BufferedReader responder = null;

    public SocketKvsConnection(ConnectionConfig config) {
        this.config = config;
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
            lazyInitializer();
            RespWriter rw = new RespWriter(socket.getOutputStream());
            rw.write(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

//            String result = "";
//            String k;
//            while ((k = in.readLine()) != null) {
//                result += k + "\r\n";
//            }
            //RespReader rr = new RespReader(new ByteArrayInputStream(result.getBytes()));
            RespReader rr = new RespReader(socket.getInputStream());
            return rr.readObject();
        } catch (IOException e) {
            throw new ConnectionException("Connection was not established or lost.", e);
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

    private void lazyInitializer() throws IOException {
        if ((this.socket == null) || (!this.socket.isConnected())) {
            this.socket = new Socket(config.getHost(), config.getPort());
            requester = new PrintWriter(socket.getOutputStream(), true);
            responder = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }
    }

}
