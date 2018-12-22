package com.authrus.database;

import java.util.concurrent.atomic.AtomicLong;

public class Sequence {
   
   private final AtomicLong sequence;
   private final String name;
   
   public Sequence(String name) {
      this.sequence = new AtomicLong();
      this.name = name;
   }

   public long update(long value) {      
      long current = sequence.get();
      
      while(value > current) {
         if(sequence.compareAndSet(current, value)) {
            return value; 
         }         
         current = sequence.get();
      }
      return sequence.get();
   }
   
   public long next() {      
      return sequence.getAndIncrement();
   }
   
   @Override
   public String toString() {
      return String.format("%s:%s", name, sequence);
   }
}
