package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

public class GetKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "GET_KEY";

    private Integer commandId;
    private String databaseName;
    private String tableName;
    private String key;

    public GetKvsCommand(String databaseName, String tableName, String key) {
        this.commandId = idGen.getAndIncrement();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
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
        return new RespArray(rCommandId, rCommandName, rDatabaseName, rTableName, rKey);
    }

    @Override
    public int getCommandId() {
        return this.commandId;
    }
}
