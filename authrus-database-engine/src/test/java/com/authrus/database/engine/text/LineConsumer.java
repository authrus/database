package com.authrus.database.engine.text;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class LineConsumer {   

   private final BlockingQueue<Line> records;
   private final LineParser parser;
   private final AtomicInteger counter;
   private final InputStream source;
   private final String name;
   private final byte[] buffer;
   
   public LineConsumer(InputStream source, String name) {
      this(source, name, 8192);
   }
   
   public LineConsumer(InputStream source, String name, int capacity) {
      this.records = new LinkedBlockingQueue<Line>();
      this.parser = new LineParser();
      this.counter = new AtomicInteger();
      this.buffer = new byte[capacity];
      this.source = source;
      this.name = name;
   }
   
   public Line read() throws IOException {
      Line record = records.poll();
      
      while(record == null) {
         int count = source.read(buffer);
               
         if(count < 0) {
            return null;
         }
         List<String> lines = parser.update(buffer, 0, count);  
         
         for(String line : lines) {
            int next = counter.getAndIncrement();  
            
            if(line == null) {
               throw new IllegalStateException("Record from '" + name + "' was null");
            }
            record = new Line(line, name, next);            
            
            if(!records.offer(record)) {
               throw new IllegalStateException("Unable to queue record '" + record + "' from '" + name + "'");
            }            
         }
         record = records.poll();   
      }
      return record;      
   }
}
