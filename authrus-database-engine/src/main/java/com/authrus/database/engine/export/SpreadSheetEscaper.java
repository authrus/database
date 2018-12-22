package com.authrus.database.engine.export;

public class SpreadSheetEscaper implements ValueEscaper {
   
   private static final char[] SPECIALS = new char[] {'\r', '\n', '"', ','};
   
   private final StringBuilder builder;   
   
   public SpreadSheetEscaper() {     
      this.builder = new StringBuilder();
   }
   
   public synchronized String escape(String value) {
      if(value != null) {
         int length = SPECIALS.length;
         
         for(int i = 0; i < length; i++) {
            char special = SPECIALS[i];
            int index = value.indexOf(special);
            
            if(index != -1) {
               return convert(value);
            }
         }
         return value;
      }
      return ""; // or "NULL"?
   }
   
   private synchronized String convert(String value) {
      int length = value.length();
      
      builder.setLength(0);
      builder.append('"');
      
      for(int i = 0; i < length; i++) {
         char next = value.charAt(i);
         
         if(next == '"') {
            builder.append(next); // double up quotes
         }
         builder.append(next);        
      }
      builder.append('"');
      
      return builder.toString();
   }   
}
