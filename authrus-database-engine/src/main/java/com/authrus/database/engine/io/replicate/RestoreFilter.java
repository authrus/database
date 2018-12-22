package com.authrus.database.engine.io.replicate;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionFilter;

public class RestoreFilter implements TransactionFilter {
   
   private static final Logger LOG = LoggerFactory.getLogger(RestoreFilter.class);
   
   private final PositionRecorder recorder;
   private final AtomicInteger counter;
   private final AtomicLong last;
   private final String owner;
   private final int interval;

   public RestoreFilter(Position position, String owner) {
      this(position, owner, 10000);      
   }
   
   public RestoreFilter(Position position, String owner, int interval) {
      this.recorder = new PositionRecorder(position, owner);
      this.counter = new AtomicInteger();
      this.last = new AtomicLong();
      this.interval = interval;
      this.owner = owner;
   }

   @Override
   public boolean accept(Transaction transaction) {      
      int count = counter.getAndIncrement();
      
      if(count % interval == 0) {
         long current = System.currentTimeMillis();
         long previous = last.get();       
         
         if(previous > 0) {
            LOG.info("Restored owner=[" + owner + "] count=[" + count + "] duration=[" + (current-previous) + "]");
         }
         last.set(current);
      }
      return recorder.accept(transaction);
   }

}
