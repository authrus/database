package com.authrus.database.engine.io;

import java.io.ByteArrayOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import com.authrus.database.common.io.DataReader;
import com.authrus.database.common.io.OutputStreamWriter;

public class DataRecordBufferTest extends TestCase {
   
   public void testDribbleRecords() throws Exception {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      OutputStreamRecordProducer producer = new OutputStreamRecordProducer(output);
      DataRecordOutputStream stream = new DataRecordOutputStream(producer);      
      List<String> tokens = new ArrayList<String>();
      
      for(int i = 0; i < 1000; i++) {
         tokens.add("blah-blah-blah-"+i);
      }
      for(String token : tokens) {
         ByteArrayOutputStream recordBuffer = new ByteArrayOutputStream();
         OutputStreamWriter encoder = new OutputStreamWriter(recordBuffer);
         DataRecordWriter writer = new DataRecordWriter(encoder);
         
         writer.writeString(token);
         byte[] b = recordBuffer.toByteArray();
         stream.write(b);         
         stream.flush();
      }      
      byte[] result = output.toByteArray();
      List<DataReader> total = new ArrayList<DataReader>();
      DataRecordParser buffer = new DataRecordParser("test");
      Random random = new SecureRandom();
      int remaining = result.length;
      int pos = 0;
      
      while(remaining > 0) {
         int next = random.nextInt(100)+1;
         int size = Math.min(remaining, next);
         
         List<DataReader> records = buffer.update(result, pos, next);
         total.addAll(records);
         pos+=size;
         remaining-=size;
      }
      int index = 0;
      
      for(DataReader item : total) {
         DataRecordReader reader = new DataRecordReader(item);
         String text = reader.readString();        
         assertEquals(text, tokens.get(index++));
      }   
   }
   

   public void testManyRecords() throws Exception {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      OutputStreamRecordProducer producer = new OutputStreamRecordProducer(output);
      DataRecordOutputStream stream = new DataRecordOutputStream(producer);    
      List<String> tokens = new ArrayList<String>();
      
      for(int i = 0; i < 1000; i++) {
         tokens.add("blah-blah-blah-"+i);
      }
      for(String token : tokens) {
         ByteArrayOutputStream recordBuffer = new ByteArrayOutputStream();
         OutputStreamWriter encoder = new OutputStreamWriter(recordBuffer);
         DataRecordWriter writer = new DataRecordWriter(encoder);
         
         writer.writeString(token);
         byte[] b = recordBuffer.toByteArray();
         stream.write(b);  
         stream.flush();
      }       
      byte[] result = output.toByteArray();
      DataRecordParser buffer = new DataRecordParser("test");
      List<DataReader> records = buffer.update(result, 0, result.length);
      int index = 0;
      
      for(DataReader item : records) {
         DataRecordReader reader = new DataRecordReader(item);
         String text = reader.readString();  
         assertEquals(text, tokens.get(index++));
      }   
   }
   
   public void testRecordBuffer() throws Exception {     
      ByteArrayOutputStream recordBuffer = new ByteArrayOutputStream();
      OutputStreamWriter encoder = new OutputStreamWriter(recordBuffer);
      DataRecordWriter writer = new DataRecordWriter(encoder);
      
      writer.writeString("Hello World!");
      byte[] array = recordBuffer.toByteArray();
      for(int i = 0; i < array.length; i++){
         System.err.println("["+i+"]="+array[i]);
      }
      
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      OutputStreamRecordProducer producer = new OutputStreamRecordProducer(output);
      DataRecordOutputStream stream = new DataRecordOutputStream(producer); 
      
      stream.write(array);  
      stream.flush();
      System.err.println();
      
      byte[] result = output.toByteArray();
      for(int i = 0; i < 12; i++){
         System.err.println("["+i+"]="+result[i]+" HEADER");
      }
      for(int i = 12, j = 0; i < result.length; i++, j++){
         System.err.println("["+j+"]["+i+"]="+result[i]+" BODY");
      }
      
      DataRecordParser buffer = new DataRecordParser("test");
      List<DataReader> records = buffer.update(result, 0, result.length);
      
      for(DataReader item : records) {
         DataRecordReader reader = new DataRecordReader(item);
         String text = reader.readString();        
         assertEquals(text, "Hello World!");
      }      
   }

}
