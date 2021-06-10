package com.itmo.java.client.client;

import com.itmo.java.client.command.KvsCommand;
import com.itmo.java.client.command.CreateDatabaseKvsCommand;
import com.itmo.java.client.command.CreateTableKvsCommand;
import com.itmo.java.client.command.GetKvsCommand;
import com.itmo.java.client.command.SetKvsCommand;
import com.itmo.java.client.command.DeleteKvsCommand;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {

    private String databaseName;
    private KvsConnection session;

    /**
     * Констурктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания коннекшена к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.session = connectionSupplier.get();
    }

    private String runCommand(KvsCommand c) throws DatabaseExecutionException {
        RespObject response;
        try {
            response = session.send(c.getCommandId(), c.serialize());
            if (response.isError()) {
                throw new DatabaseExecutionException("Error from server: " + response.asString());
            }
            return response.asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("KvsClient has failed to connect to the server.", e);
        }
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        KvsCommand c = new CreateDatabaseKvsCommand(databaseName);
        return runCommand(c);
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        KvsCommand c = new CreateTableKvsCommand(databaseName, tableName);
        return runCommand(c);
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand c = new GetKvsCommand(databaseName, tableName, key);
        return runCommand(c);
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        KvsCommand c = new SetKvsCommand(databaseName, tableName, key, value);
        return runCommand(c);
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand c = new DeleteKvsCommand(databaseName, tableName, key);
        return runCommand(c);
    }
}
