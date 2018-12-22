package com.authrus.database.engine.text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LineParser {
   
   private StringBuilder builder;
   private byte[] buffer;
   private int count;        

   public LineParser() {
      this(1048576);
   }
   
   public LineParser(int capacity) {
      this.builder = new StringBuilder();
      this.buffer = new byte[8];
   }
   
   public List<String> update(byte[] array) throws IOException {
      return update(array, 0, array.length);
   }
   
   public List<String> update(byte[] array, int off, int length) {
      List<String> records = new ArrayList<String>();
      
      if(count > 0) {
         int peek = buffer[0];         
         int require = length(peek);
         
         while(require > count) {
            if(length <= 0) {
               break;
            }
            buffer[count] = array[off];
            count++;
            length--;           
            off++;
         }
         if(require == count) {
            int code = decode(buffer, 0, require);
            
            builder.appendCodePoint(code);
            count = 0;
         }        
      }                         
      int last = length + off;
      
      while(off < last) {
         int peek = array[off];  
         int require = length(peek);
         
         if(off + require > last) {
            int remain = last - off;
            
            if(remain > 0) {
               System.arraycopy(array, off, buffer, 0, remain);
               count = remain;
            }
            break;
         }
         int code = decode(array, off, require);
         int size = builder.length();
         
         if(code == '\n' || code == '\r') {
            if(size > 0) {
               String record = builder.toString();                          
           
               records.add(record);            
               builder.setLength(0);
            }
         } else {
            builder.appendCodePoint(code);
         }
         off += require;
      }
      return records;
   }
   
   private int length(int peek) {
      if((peek & 0x80) == 0x00){
         return 1;
      } else if((peek & 0xe0) == 0xc0){
         return 2;
      } else if((peek & 0xf0) == 0xe0){
         return 3;
      } else if((peek & 0xf8) == 0xf0){
         return 4;
      } else if((peek & 0xfc) == 0xf8){
         return 5;
      } else if((peek & 0xfe) == 0xfc){
         return 6;
      } else {
         String binary = Integer.toBinaryString(peek);
         int length = binary.length();
         
         for(int i = length; i < 32; i++) {
            binary = "0" + binary;
         }
         throw new IllegalStateException("Prefix '" + binary + "' is not valid UTF-8");
      } 
   }
   
   private int decode(byte[] array, int off, int count) {
      int value = array[off];
      
      if(count == 1) {
         value = value & 0xff;
      } else if(count == 2){
         value = value & 0x1f;
      } else if(count == 3) {
         value = value & 0x0f;
      } else if(count == 4) {
         value = value & 0x07;
      } else if(count == 5) {
         value = value & 0x03;
      } else if(count == 6) {
         value = value & 0x01;
      } else {
         String binary = Integer.toBinaryString(value);
         int length = binary.length();
         
         for(int i = length; i < 32; i++) {
            binary = "0" + binary;
         }
         throw new IllegalStateException("Prefix '" + binary + "' is not valid UTF-8");
      }      
      for(int i = 1; i < count; i++) {         
         int next = array[off + i];

         if((next & 0xc0) != 0x80){
            throw new IllegalStateException("Data is not in UTF-8 format");
         }
         value = (value<<6)|(next&0x3f);                                 
      }        
      return value;
   }      
} 