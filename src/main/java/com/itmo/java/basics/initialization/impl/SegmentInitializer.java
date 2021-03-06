package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

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

        if (context.currentSegmentContext() == null) {
            throw new DatabaseException("currentSegmentContext is null. Must be initialized.");
        }

        long offset = 0;
        Set<String> keys = new HashSet<>();

        try {
            InputStream ioStream = new FileInputStream(context.currentSegmentContext().getSegmentPath().toAbsolutePath().toString());
            DatabaseInputStream dbStream = new DatabaseInputStream(ioStream);
            long segmentSize = Files.size(context.currentSegmentContext().getSegmentPath());

            DatabaseRecord record;
            while (offset < segmentSize) {
                record = dbStream.readDbUnit().get();
                String key = new String(record.getKey());
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(key, new SegmentOffsetInfoImpl(offset));
                offset += record.size();
                keys.add(key);
            }
        } catch (Exception e) {
            throw new DatabaseException("Segment could not be instantiated.", e);
        }

        SegmentInitializationContext subContext = new SegmentInitializationContextImpl(
                context.currentSegmentContext().getSegmentName(),
                context.currentSegmentContext().getSegmentPath(),
                (int) offset,
                context.currentSegmentContext().getIndex()
        );
        context = new InitializationContextImpl(context.executionEnvironment(), context.currentDbContext(), context.currentTableContext(), subContext);

        Segment segment = SegmentImpl.initializeFromContext(context.currentSegmentContext());
        if (context.currentTableContext() != null) {
            for (String key : keys) {
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
            }
            context.currentTableContext().updateCurrentSegment(segment);
        }

    }
}
