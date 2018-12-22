package com.authrus.database.engine.text;

public class Line {

   private final String source;
   private final String line;
   private final int count;
   
   public Line(String record, String source, int count) {
      this.line = record;
      this.source = source;
      this.count = count;
   }  
   
   public String getSource() {
      return source;
   }   
   
   public String getText() {
      return line;
   }
   
   public long getCount() {
      return count;
   }
   
   @Override
   public String toString() {
      return String.format("%s -> %s", source, line);
   }
}
