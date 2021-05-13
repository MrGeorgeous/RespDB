package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

/**
 * Команда для создания таблицы
 */
public class CreateTableKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_TABLE";

    private Integer commandId;
    private String databaseName;
    private String tableName;

    public CreateTableKvsCommand(String databaseName, String tableName) {
        this.commandId = idGen.getAndIncrement();
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        RespCommandId rCommandId = new RespCommandId(getCommandId());
        RespBulkString rCommandName = new RespBulkString(COMMAND_NAME.getBytes());
        RespBulkString rDatabaseName = new RespBulkString(databaseName.getBytes());
        RespBulkString rTableName = new RespBulkString(tableName.getBytes());
        return new RespArray(rCommandId, rCommandName, rDatabaseName, rTableName);
    }

    @Override
    public int getCommandId() {
        return this.commandId;
    }
}
