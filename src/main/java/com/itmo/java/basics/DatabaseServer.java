package com.itmo.java.basics;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;

import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private ExecutionEnvironment environment;

    /**
     * Constructor
     *
     * @param env         env для инициализации. Далее работа происходит с заполненым объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        InitializationContext initContext = new InitializationContextImpl(env, null, null, null);
        initializer.perform(initContext);
        return new DatabaseServer(initContext.executionEnvironment());
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        String commandName = message.getObjects().get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString();
        DatabaseCommand command = DatabaseCommands.valueOf(commandName).getCommand(environment, message.getObjects());
        return executeNextCommand(command);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }

    public ExecutionEnvironment getEnv() {
        return this.environment;
    }

    private DatabaseServer(ExecutionEnvironment env) {
        this.environment = env;
    }
}