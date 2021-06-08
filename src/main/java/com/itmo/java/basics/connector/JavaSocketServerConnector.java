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
        try {
            this.databaseServer = databaseServer;
            this.serverSocket = new ServerSocket(config.getPort());
        } catch (Exception e) {
            throw new IOException("ServerSocket could not be opened.", e);
        }
    }
 
     /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {


//        connectionAcceptorExecutor.submit(() -> {
//            if ((serverSocket != null) && !serverSocket.isClosed()) {
//                clientIOWorkers.submit(new ClientTask(s, databaseServer));
//            }
//                    } catch (IOException e) {
//                        //e.printStackTrace();
//                    }
//        });

        while(!Thread.currentThread().isInterrupted()) {
            //final Socket s = null;
            try {
                final Socket s = serverSocket.accept();
                connectionAcceptorExecutor.submit(() -> {
                    //try {
                        if ((serverSocket != null) && !serverSocket.isClosed()) {
                            clientIOWorkers.submit(new ClientTask(s, databaseServer));
                        }
//                    } catch (IOException e) {
//                        //e.printStackTrace();
//                    }
                });
            } catch (Exception e) {
                //s.close();
            }

        }

    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        connectionAcceptorExecutor.shutdownNow();
        clientIOWorkers.shutdownNow();
        try {
            //System.out.println("Stopping socket connector");
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
        private CommandReader reader = null;
        private RespWriter writer = null;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.clientSocket = client;
            this.server = server;
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
            try {
                if (reader == null) {
                    reader = new CommandReader(new RespReader(clientSocket.getInputStream()), server.getEnvironment());
                }
                if (writer == null) {
                    writer = new RespWriter(clientSocket.getOutputStream());
                }
                while (clientSocket.isConnected() && !clientSocket.isInputShutdown() && !clientSocket.isOutputShutdown()) {
                    if (reader.hasNextCommand()) {

//                        clientSocket.getOutputStream().write(111);
//                        continue;

                        //System.out.println("Getting command");
                        //DatabaseCommandResult result = server.executeNextCommand(reader.readCommand()).join().serialize();

                        writer.write(server.executeNextCommand(reader.readCommand()).join().serialize());
                        clientSocket.getOutputStream().flush();


//                        RespObject result;
//                        try {
//                            result = server.executeNextCommand(reader.readCommand()).join().serialize();
//                        } catch (Exception e) {
//                            result = new RespError(e.getMessage().getBytes());
//                        }
//                        //result.write(System.out);
//                        writer.write(result);

                    } else {
                        break;
                    }
                }
            } catch (Exception ignored) {
                //ignored.printStackTrace();
                //System.out.println("Failed to process request.");
            }
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
