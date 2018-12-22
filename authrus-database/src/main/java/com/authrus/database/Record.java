package com.authrus.database;

import java.util.Date;
import java.util.Set;

public interface Record {
   Set<String> getColumns() throws Exception;
   Integer getInteger(String name) throws Exception;
   String getString(String name) throws Exception;
   Double getDouble(String name) throws Exception;
   Float getFloat(String name) throws Exception;   
   Long getLong(String name) throws Exception;  
   Date getDate(String name) throws Exception; 
   Boolean getBoolean(String name) throws Exception;
   Byte getByte(String name) throws Exception;
   Character getCharacter(String name) throws Exception;
   Short getShort(String name) throws Exception;   
   
}
