package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

/**
 * Команда для создания бд
 */
public class CreateDatabaseKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_DATABASE";

    private Integer commandId;
    private String databaseName;

    /**
     * Создает объект
     *
     * @param databaseName имя базы данных
     */
    public CreateDatabaseKvsCommand(String databaseName) {
        this.commandId = idGen.getAndIncrement();
        this.databaseName = databaseName;
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
        return new RespArray(rCommandId, rCommandName, rDatabaseName);
    }

    @Override
    public int getCommandId() {
        return this.commandId;
    }
}
