package com.authrus.database;

import java.util.concurrent.atomic.AtomicInteger;

public class Counter {
   
   private final AtomicInteger counter;
   
   public Counter() {
      this(0);
   }
   
   public Counter(int start) {
      this.counter = new AtomicInteger(start);
   }
   
   public int get() {
      return counter.get();
   }
   
   public int next() {
      return counter.incrementAndGet();
   }
}
