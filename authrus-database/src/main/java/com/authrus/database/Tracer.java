package com.authrus.database;

public interface Tracer<T> {
   void trace(T value, long duration);
}
