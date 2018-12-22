package com.authrus.database.engine.io;

import com.authrus.database.common.io.DataReader;

public class DataRecord {

   private final DataRecordReader reader;
   private final String name;  
   private final long time;
   
   public DataRecord(DataReader reader, String name, long time) {
      this.reader = new DataRecordReader(reader);
      this.name = name;
      this.time = time;
   }
   
   public DataRecordReader getReader() {
      return reader;
   }
   
   public String getName() {
      return name;
   }
   
   public long getTime() {
      return time;
   }
   
   @Override
   public String toString() {
      return String.format("%s: %s", name, time);
   }
}
