package com.authrus.database.bind.table.statement;

import com.authrus.database.Record;

public interface RecordMapper<T> {
   Record fromObject(T object) throws Exception;
   T toObject(Record record) throws Exception;
}
