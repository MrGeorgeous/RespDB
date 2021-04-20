package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;


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
        SegmentImpl.SegmentBuilder s = new SegmentImpl.SegmentBuilder(context.currentSegmentContext().getSegmentName(), context.currentSegmentContext().getSegmentPath(), context.currentSegmentContext().getCurrentSize());
        s.instantiate();
        Segment segment = s.build();
        context.currentTableContext().updateCurrentSegment(segment);
        for (String key : s.getKeys()) {
            context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
        }
    }
}
