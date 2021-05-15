package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {

    private ExecutionEnvironment environment;
    private List<RespObject> commandArgs;

    /**
     * Создает команду
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, имя таблицы
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.environment = env;
        this.commandArgs = commandArgs;
        if ((this.environment == null) || (this.commandArgs == null) || (this.commandArgs.size() != 4)) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Создает таблицу в нужной бд
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = this.commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = this.commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            Optional<Database> db = environment.getDatabase(dbName);
            if (db.isPresent()) {
                db.get().createTableIfNotExists(tableName);
            } else {
                return new FailedDatabaseCommandResult("Database '" + dbName + "' does not exist.");
            }
            return new SuccessDatabaseCommandResult(("Table '" + tableName + "' has been created.").getBytes());
        } catch (Exception e) {
            return new FailedDatabaseCommandResult("Table has not been created. Stacktrace: " + e.getMessage());
        }
    }
}
