package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;

/**
 * Команда для создания базы данных
 */
public class CreateDatabaseCommand implements DatabaseCommand {

    private ExecutionEnvironment environment;
    private DatabaseFactory factory;
    private List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param factory     функция создания базы данных (пример: DatabaseImpl::create)
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя создаваемой бд
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<RespObject> commandArgs) {
        this.environment = env;
        this.factory = factory;
        this.commandArgs = commandArgs;
        if ((this.environment == null) || (this.factory == null) || (this.commandArgs == null) || (this.commandArgs.size() != 3)) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Создает бд в нужном env
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная база была создана. Например, "Database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = this.commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            environment.addDatabase(factory.createNonExistent(dbName, environment.getWorkingPath()));
            return new SuccessDatabaseCommandResult(("Database '" + dbName + "' has been created.").getBytes());
        } catch (Exception e) {
            return new FailedDatabaseCommandResult("Database has not been created. Stacktrace: " + e.getMessage());
        }
    }
}
