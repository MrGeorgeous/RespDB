package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Inet4Address;
import java.net.InetAddress;
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

    private final ServerSocket serverSocket;
    private final DatabaseServer databaseServer;

    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        this.databaseServer = databaseServer;

        InetAddress addr = Inet4Address.getByName(config.getHost());
        this.serverSocket = new ServerSocket(config.getPort(), 50, addr);
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            while (true) {
                Socket socket = serverSocket.accept();
                ClientTask task = new ClientTask(socket, databaseServer);
                clientIOWorkers.submit(task);
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");

        connectionAcceptorExecutor.shutdownNow();
        clientIOWorkers.shutdownNow();

        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
        private final Socket socket;
        private final DatabaseServer dbServer;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.socket = client;
            this.dbServer = server;
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
            try (CommandReader reader = new CommandReader(new RespReader(socket.getInputStream()), dbServer.getEnvironment());
                 RespWriter writer = new RespWriter(socket.getOutputStream())) {

                while (reader.hasNextCommand()) {
                    DatabaseCommand command = reader.readCommand();
                    DatabaseCommandResult result = this.dbServer.executeNextCommand(command).get();
                    writer.write(result.serialize());
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
