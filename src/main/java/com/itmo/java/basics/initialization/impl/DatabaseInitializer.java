package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;

public class DatabaseInitializer implements Initializer {
    protected Initializer subInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.subInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {

        if (initialContext.executionEnvironment() == null) {
            throw new DatabaseException("executionEnvironment is null. Must be initialized.");
        }

        if (initialContext.currentDbContext() == null) {
            throw new DatabaseException("currentDbContext is null. Must be initialized.");
        }

        File dbDirectory = new File(initialContext.currentDbContext().getDatabasePath().toString());
        if (!dbDirectory.exists() && !dbDirectory.mkdir()) {
            throw new DatabaseException("Database directory could not be accessed or created.");
        }

        InitializationContext tableContext = initialContext;
        String[] tables = dbDirectory.list((current, name) -> new File(current, name).isDirectory());
        for (String tableName : tables) {
            TableInitializationContext subContext = new TableInitializationContextImpl(tableName, dbDirectory.toPath(), new TableIndex());
            tableContext = new InitializationContextImpl(initialContext.executionEnvironment(), initialContext.currentDbContext(), subContext, null);
            this.subInitializer.perform(tableContext);
        }

        initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(tableContext.currentDbContext()));

    }
}
