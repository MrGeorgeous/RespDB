package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Path;

public class DatabaseServerInitializer implements Initializer {
    protected Initializer subInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.subInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        if (context.executionEnvironment() == null) {
            throw new DatabaseException("executionEnvironment is null. Must be initialized.");
        }

        File dbsDirectory = new File(context.executionEnvironment().getWorkingPath().toString());
        if (!dbsDirectory.exists() && !dbsDirectory.mkdir()) {
            throw new DatabaseException("Databases directory could not be accessed or created.");
        }

        String[] databases = dbsDirectory.list((current, name) -> new File(current, name).isDirectory());
        for (String databaseName : databases) {
            DatabaseInitializationContext subContext = new DatabaseInitializationContextImpl(databaseName, dbsDirectory.toPath());
            context = new InitializationContextImpl(context.executionEnvironment(), subContext, null, null);
            this.subInitializer.perform(context);
        }

    }
}
