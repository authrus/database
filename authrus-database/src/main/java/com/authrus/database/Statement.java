package com.authrus.database;

import java.util.Date;

public interface Statement {
   ResultIterator<Record> execute() throws Exception;
   Statement set(String name, String value) throws Exception;
   Statement set(String name, Integer value) throws Exception;
   Statement set(String name, Long value) throws Exception;  
   Statement set(String name, Double value) throws Exception;
   Statement set(String name, Float value) throws Exception;
   Statement set(String name, Boolean value) throws Exception;
   Statement set(String name, Character value) throws Exception;
   Statement set(String name, Byte value) throws Exception;
   Statement set(String name, Short value) throws Exception;   
   Statement set(String name, Date value) throws Exception;   
   Statement close() throws Exception;
}
