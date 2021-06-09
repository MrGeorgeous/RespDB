package com.itmo.java.basics.resp;

import com.itmo.java.basics.console.*;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.util.List;

public class CommandReader implements AutoCloseable {

    private RespReader respReader;
    private ExecutionEnvironment environment;

    public CommandReader(RespReader reader, ExecutionEnvironment env) {
        this.respReader = reader;
        this.environment = env;
    }

    /**
     * Есть ли следующая команда в ридере?
     */
    public boolean hasNextCommand() throws IOException {
        return respReader.hasArray();
    }

    /**
     * Считывает комманду с помощью ридера и возвращает ее
     *
     * @throws IllegalArgumentException если нет имени команды и id
     */
    public DatabaseCommand readCommand() throws IOException {
        List<RespObject> args;
        try  {
            args = respReader.readArray().getObjects();
        } catch (Exception e) {
            throw new IOException("No next command presented in Resp", e);
        }
        try {
            RespObject commandId = (RespCommandId) args.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex());
            String commandName = args.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString();
            return DatabaseCommands.valueOf(commandName).getCommand(environment, args);
        } catch (Exception e) {
            throw new IllegalArgumentException("Given array does not contain commandId and commandName.", e);
        }
    }

    @Override
    public void close() throws Exception {
        respReader.close();
    }
}
