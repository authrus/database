package com.authrus.database.engine.predicate;

public class LikeEvaluator {
   
   private Comparable value;
   private char[] wild;
   
   public LikeEvaluator(Comparable value) {
      this.value = value;
   }

   public boolean like(String text) {
      if(text != null) {
         if(wild == null) {
            String token = String.valueOf(value);
            String lower = token.toLowerCase();
            
            wild = lower.toCharArray();
         }
         return like(text, 0, 0);
      }
      return false;
   }
   
   private boolean like(String text, int off, int pos){
      int length = text.length();
      
      while(pos < wild.length && off < length){ // examine chars 
         if(wild[pos] == '%'){
            while(wild[pos] == '%'){ // totally wild 
               if(++pos >= wild.length) // if finished 
                  return true;
            }
            if(wild[pos] == '_') { // *_ is special 
               if(++pos >= wild.length)                    
                  return true;
            }
            for(; off < length; off++){ // find next matching char
               char next = text.charAt(off);
               char code = Character.toLowerCase(next);
               
               if(code == wild[pos] || wild[pos] == '_'){ // match 
                  if(wild[pos - 1] != '_'){
                     if(like(text, off, pos))
                        return true;
                  } else {
                     break;                          
                  }
               }
            }
            if(length == off)
               return false;
         }
         char next = text.charAt(off++);
         char code = Character.toLowerCase(next);
         
         if(code != wild[pos++]){
            if(wild[pos-1] != '_')
               return false; // if not equal 
         }
      }
      if(wild.length == pos){ // if wild is finished 
          return length == off; // is text finished 
      }
      while(wild[pos] == '%'){ // ends in all % 
         if(++pos >= wild.length) // if finished 
            return true;
      }
      return false;
   }
}
