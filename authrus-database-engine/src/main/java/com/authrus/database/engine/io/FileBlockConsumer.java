package com.authrus.database.engine.io;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FileBlockConsumer implements DataBlockConsumer {
   
   private final BlockingQueue<DataBlock> blocks;
   private final FileWatcher watcher;
   private final FileCursor cursor;
   private final FileSeeker seeker;
   private final String directory;

   public FileBlockConsumer(FileSeeker seeker, String directory) {
      this.blocks = new ArrayBlockingQueue<DataBlock>(10);
      this.watcher = new FileWatcher(directory);
      this.cursor = new FileCursor();
      this.directory = directory;
      this.seeker = seeker;      
   }

   @Override
   public DataBlock read(long wait) {
      try {
         while(blocks.isEmpty()) {           
            FilePath update = watcher.next(wait);

            if(update == null) {
               return null;
            }
            if(seeker.accept(update)) {
               List<DataBlock> changes = cursor.readBlocks(update);
               
               for(DataBlock change : changes) {
                  if(change != null) {
                     blocks.offer(change);
                  }
               }            
            }
         }
      } catch (Exception e) {
         throw new IllegalStateException("Error waiting for change to " + directory, e);
      }
      return blocks.poll();
   }

   public void start() {
      watcher.start();
   }

   public void stop()  {
      watcher.stop();
   }
}
