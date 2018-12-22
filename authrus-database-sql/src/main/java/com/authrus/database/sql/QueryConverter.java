package com.authrus.database.sql;

public interface QueryConverter<T> {
    T convert(Query command);
}
