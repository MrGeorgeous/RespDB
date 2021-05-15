package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {

    private ExecutionEnvironment environment;
    private List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.environment = env;
        this.commandArgs = commandArgs;
        if ((this.environment == null) || (this.commandArgs == null) || (this.commandArgs.size() != 5)) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = this.commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = this.commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String key = this.commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            Optional<Database> db = environment.getDatabase(dbName);
            if (db.isPresent()) {
                Optional<byte[]> value = db.get().read(tableName, key);
                db.get().delete(tableName, key);
                return new SuccessDatabaseCommandResult(value.get());
            } else {
                return new FailedDatabaseCommandResult("Database '" + dbName + "' does not exist.");
            }
        } catch (Exception e) {
            return new FailedDatabaseCommandResult("Table or key to delete has not been found. Stacktrace: " + e.getMessage());
        }
    }
}
