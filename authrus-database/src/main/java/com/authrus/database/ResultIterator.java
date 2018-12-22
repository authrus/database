package com.authrus.database;

import java.util.List;

public interface ResultIterator<T> {
   T next() throws Exception;
   T fetchFirst() throws Exception;
   T fetchLast() throws Exception;     
   List<T> fetchAll() throws Exception;   
   List<T> fetchNext(int count) throws Exception;
   boolean hasMore() throws Exception;
   boolean isEmpty() throws Exception;
   void close() throws Exception;
}
