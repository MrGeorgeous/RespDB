package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class TableInitializer implements Initializer {

    private SegmentInitializer subInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.subInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

//        if (context.executionEnvironment() == null) {
//            throw new DatabaseException("executionEnvironment is null. Must be initialized.");
//        }

        if (context.currentDbContext() == null) {
            throw new DatabaseException("currentDbContext is null. Must be initialized.");
        }

        if (context.currentTableContext() == null) {
            throw new DatabaseException("currentTableContext is null. Must be initialized.");
        }

        String tableName = context.currentTableContext().getTableName();
        File tableDirectory = new File(context.currentTableContext().getTablePath().toString());

        if (!tableDirectory.exists() && !tableDirectory.mkdir()) {
            throw new DatabaseException("Table directory could not be accessed or created.");
        }

        InitializationContext segmentContext = context;
        String[] segments = tableDirectory.list((current, name) -> new File(current, name).isFile());
        Arrays.sort(segments);
        for (String segmentName : segments) {

            Path segment = tableDirectory.toPath().resolve(segmentName);
            SegmentInitializationContext subContext = null;
            subContext = new SegmentInitializationContextImpl(segmentName, tableDirectory.toPath().resolve(segmentName), 0, new SegmentIndex());
            segmentContext = new InitializationContextImpl(context.executionEnvironment(), context.currentDbContext(), context.currentTableContext(), subContext);
            this.subInitializer.perform(segmentContext);

        }

        context = segmentContext;
        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));

    }
}
