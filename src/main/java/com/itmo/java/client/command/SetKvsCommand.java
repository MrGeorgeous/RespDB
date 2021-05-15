package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

public class SetKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "SET_KEY";

    private Integer commandId;
    private String databaseName;
    private String tableName;
    private String key;
    private String value;

    public SetKvsCommand(String databaseName, String tableName, String key, String value) {
        this.commandId = idGen.getAndIncrement();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
        this.value = value;
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
        RespBulkString rKey = new RespBulkString(key.getBytes());
        RespBulkString rValue = new RespBulkString(value.getBytes());
        return new RespArray(rCommandId, rCommandName, rDatabaseName, rTableName, rKey, rValue);
    }

    @Override
    public int getCommandId() {
        return this.commandId;
    }
}
