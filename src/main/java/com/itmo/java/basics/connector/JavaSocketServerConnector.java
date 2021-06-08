package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.config.DatabaseServerConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();

    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final DatabaseServer databaseServer;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        //try {
        this.databaseServer = databaseServer;
        this.serverSocket = new ServerSocket(config.getPort());
        //} catch (Exception e) {
        //    throw new IOException("ServerSocket could not be opened.", e);
        //}
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {

        connectionAcceptorExecutor.submit(() -> {
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    Socket s = serverSocket.accept();
                    clientIOWorkers.submit(new ClientTask(s, databaseServer));
                } catch (Exception e) {
                    break;
                    //throw new RuntimeException("hahaha", e);
                }
            }
        });


    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        try {
            System.out.println("Stopping socket connector");
            connectionAcceptorExecutor.shutdownNow();
            clientIOWorkers.shutdownNow();
            serverSocket.close();
        } catch (Exception e) {

        }
    }


    public static void main(String[] args) throws Exception {

        ConfigLoader configLoader = new ConfigLoader();
        DatabaseServerConfig config = configLoader.readConfig();
        ExecutionEnvironment env = new ExecutionEnvironmentImpl(config.getDbConfig());
        DatabaseServerInitializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(
                        new TableInitializer(
                                new SegmentInitializer())));
        DatabaseServer dbServer = DatabaseServer.initialize(env, initializer);

        JavaSocketServerConnector server = new JavaSocketServerConnector(dbServer, config.getServerConfig());
        server.start();

    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {

        private Socket clientSocket = null;
        private DatabaseServer server = null;
        private RespReader reader = null;
        private RespWriter writer = null;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.clientSocket = client;
            this.server = server;
            try {
                this.reader = new RespReader(clientSocket.getInputStream());
                this.writer = new RespWriter(clientSocket.getOutputStream());
            } catch (Exception e) {
//                try {
//                    //writer.write(null);
//                    writer.write(new RespError(e.getMessage().getBytes()));
//                } catch (IOException ex) {
//                    ex.printStackTrace();
//                }
                close();
            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try (CommandReader cmdReader = new CommandReader(reader, server.getEnv())) {
                while (clientSocket.isConnected() && !Thread.currentThread().isInterrupted()) {
                    if (cmdReader.hasNextCommand()) {
                        RespObject r;
                        try {
                            r = server.executeNextCommand(cmdReader.readCommand()).join().serialize();
                            writer.write(r);
                        } catch (Exception e) {
                            writer.write(new RespError(e.getMessage().getBytes()));
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                //close();
                try {
                    writer.write(new RespError(e.getMessage().getBytes()));
                } catch (IOException ex) {

                }
                //throw new RuntimeException("hahaha2");
                //ignored.printStackTrace();
                //System.out.println("Failed to process request.");
            }
            close();
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (Exception e) {

            }
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception e) {

            }
            try {
                clientSocket.close();
            } catch (Exception e) {
                //System.out.println("Client socket was not closed.");
            }
        }

    }
}
