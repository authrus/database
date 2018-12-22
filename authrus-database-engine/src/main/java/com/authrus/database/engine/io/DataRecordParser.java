package com.authrus.database.engine.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.authrus.database.common.io.DataReader;
import com.authrus.database.common.io.InputStreamReader;

public class DataRecordParser {
   
   private ByteArrayCompressor compressor;
   private String name;
   private byte[] buffer;
   private int require;
   private int count;
   
   public DataRecordParser(String name) {
      this(name, 1048576);
   }
   
   public DataRecordParser(String name, int capacity) {
      this.compressor = new ByteArrayCompressor();
      this.buffer = new byte[capacity];
      this.name = name;
   }

   public List<DataReader> update(byte[] array) {
      return update(array, 0, array.length);
   }
   
   public List<DataReader> update(byte[] array, int off, int length) {
      List<DataReader> records = new ArrayList<DataReader>();
      ByteArrayInputStream input = new ByteArrayInputStream(array, off, length);
      
      while(length > 0) {
         if(require == 0) {
            int size = Math.min(length, 4 - count);
            int read = input.read(buffer, count, size);
            
            if(read > 0) {
               count += read;
            }
            if(read < 0) {
               return records;              
            }
            if(count >= 4) {
               require |= ((int)(buffer[0] & 0xff) << 24);
               require |= ((int)(buffer[1] & 0xff) << 16);
               require |= ((int)(buffer[2] & 0xff) << 8);
               require |= ((int)(buffer[3] & 0xff) << 0);
            }
         }
         if(require > 0) { 
            int size = Math.min(require, length);
            int read = input.read(buffer, count, size);
            
            if(read > 0) {
               count += read;
               require -= read;
            }
            if(read < 0) {
               return records;              
            }
         }
         if(require <= 0 && count >= 4) {
            byte[] result = compressor.read(buffer, 4, count);
            
            if(result.length == 0) {
               throw new IllegalStateException("Record for '" + name +"' contains no data");
            }
            InputStream stream = new ByteArrayInputStream(result);
            DataReader reader = new InputStreamReader(stream);
            
            records.add(reader);
            reset();
         }         
      }
      return records;
   }
   
   public void reset() {
      count = 0;
      require = 0;
   }
}
