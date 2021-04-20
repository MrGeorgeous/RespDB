package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

//        if (context.executionEnvironment() == null) {
//            throw new DatabaseException("executionEnvironment is null. Must be initialized.");
//        }
//
//        if (context.currentDbContext() == null) {
//            throw new DatabaseException("currentDbContext is null. Must be initialized.");
//        }

//        if (context.currentTableContext() == null) {
//            throw new DatabaseException("currentTableContext is null. Must be initialized.");
//        }

        if (context.currentSegmentContext() == null) {
            //throw new DatabaseException("currentSegmentContext is null. Must be initialized.");
        }


        SegmentImpl segment = (SegmentImpl) SegmentImpl.initializeFromContext(context.currentSegmentContext());

        try {
            DatabaseInputStream dbStream = new DatabaseInputStream(new FileInputStream(context.currentSegmentContext().getSegmentPath().toString()));
            Optional<DatabaseRecord> record;
            long offset = 0;
            while (offset < context.currentSegmentContext().getCurrentSize()) {
                record = dbStream.readDbUnit();
                if (record.isPresent() && record.get().isValuePresented()) {
                    if (context.currentSegmentContext().getIndex() != null) {
                        context.currentSegmentContext().getIndex().onIndexedEntityUpdated(new String(record.get().getKey()), new SegmentOffsetInfoImpl(offset));
                    }
                    if ((context.currentTableContext() != null) && (context.currentTableContext().getTableIndex() != null)) {
                        context.currentTableContext().getTableIndex().onIndexedEntityUpdated(new String(record.get().getKey()), segment);
                    }
                    offset += record.get().size();
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            throw new DatabaseException("Segment was found corrupted while initializing.", e);
        }

        if (context.currentTableContext() != null) {
            context.currentTableContext().updateCurrentSegment(segment);
        }

    }
}
