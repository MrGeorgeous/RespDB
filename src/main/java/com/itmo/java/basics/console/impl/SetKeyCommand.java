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
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {

    private ExecutionEnvironment environment;
    private List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.environment = env;
        this.commandArgs = commandArgs;
        if ((this.environment == null) || (this.commandArgs == null) || (this.commandArgs.size() != 6)) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = this.commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = this.commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String key = this.commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            String value = this.commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString();
            Optional<Database> db = environment.getDatabase(dbName);
            if (db.isPresent()) {
                Optional<byte[]> currentValue = db.get().read(tableName, key);
                try {
                    if ((value != null) && (value.length() != 0)) {
                        db.get().write(tableName, key, value.getBytes());
                    } else {
                        db.get().write(tableName, key, null);
                    }
                } catch (Exception e) {
                    System.out.println("hah");
                }
                if (currentValue.isPresent()) {
                    return new SuccessDatabaseCommandResult(currentValue.get());
                } else {
                    return new SuccessDatabaseCommandResult(null);
                }
            } else {
                return new FailedDatabaseCommandResult("Database '" + dbName + "' does not exist.");
            }
        } catch (Exception e) {
            return new FailedDatabaseCommandResult("Table or key to write has not been found. Stacktrace: " + e.getMessage());
        }
    }
}
