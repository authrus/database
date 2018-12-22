package com.authrus.database.engine.io.replicate;

import com.authrus.database.engine.TransactionFilter;
import com.authrus.database.engine.io.DataRecordConsumer;
import com.authrus.database.engine.io.DataRecordIterator;
import com.authrus.database.engine.io.FileBlockConsumer;
import com.authrus.database.engine.io.FileSeeker;
import com.authrus.database.engine.io.TimeFileSeeker;
import com.authrus.database.engine.io.read.ChangeProcessor;
import com.authrus.database.engine.io.read.ChangeScheduler;

public class ChangeLoader {  
   
   private final ChangeProcessor processor;
   private final TransactionFilter filter;
   private final String directory;  

   public ChangeLoader(ChangeScheduler executor, Position position, String origin, String directory) { 
      this.filter = new RestoreFilter(position, origin); 
      this.processor = new ChangeProcessor(executor, filter, true);
      this.directory = directory;
   }   
   
   public int process() throws Exception {
      FileSeeker filter = new TimeFileSeeker();
      DirectoryCleaner cleaner = new DirectoryCleaner(directory);
      FileBlockConsumer consumer = new FileBlockConsumer(filter, directory);
      DataRecordConsumer source = new DataRecordConsumer(consumer);
      DataRecordIterator iterator = new DataRecordIterator(source);

      try {
         cleaner.clean(); // remove old empty files
         consumer.start();
         return processor.process(iterator);
      } finally {
         consumer.stop();
      }
   }

}
