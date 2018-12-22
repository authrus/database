package com.authrus.database.engine.io;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.authrus.database.common.io.DataReader;

public class DataRecordConsumer {
   
   private final Map<String, DataRecordParser> parsers;
   private final BlockingQueue<DataRecord> records;
   private final DataBlockConsumer consumer;
   private final long wait;
   
   public DataRecordConsumer(DataBlockConsumer consumer) {
      this(consumer, 10000);
   }
   
   public DataRecordConsumer(DataBlockConsumer consumer, long wait) {
      this.parsers = new ConcurrentHashMap<String, DataRecordParser>();
      this.records = new LinkedBlockingQueue<DataRecord>();
      this.consumer = consumer;
      this.wait = wait;
   }
   
   public DataRecord read() {
      return read(wait);
   }

   public DataRecord read(long wait) {
      DataRecord record = records.poll();
      
      while(record == null) {
         DataBlock block = consumer.read(wait);
         
         if(block == null) {
            return null;
         }
         List<DataReader> readers = parse(block);
         String name = block.getName();
         long time = block.getTime();
         
         for(DataReader reader : readers) {
            if(readers == null) {
               throw new IllegalStateException("Record from '" + name + "' was null");
            }
            DataRecord next = new DataRecord(reader, name, time);            
            
            if(!records.offer(next)) {
               throw new IllegalStateException("Unable to queue record '" + record + "' from '" + name + "'");
            }            
         }
         record = records.poll();   
      }
      return record;      
   }
   
   private List<DataReader> parse(DataBlock block) {
      byte[] data = block.getData();
      int offset = block.getOffset();
      int length = block.getLength();
      String name = block.getName();
      DataRecordParser buffer = parsers.get(name);
         
      try {
         if(buffer == null) {
            buffer = new DataRecordParser(name);
            parsers.put(name, buffer);
         }  
         return buffer.update(data, offset, length);
      } catch(Exception e) {
         throw new IllegalStateException("Could not parse '" + name + "' of length '" + length + "'", e);
      }
   }      
}
