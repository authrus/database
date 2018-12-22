package com.authrus.database.engine.export;

import java.io.File;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import com.authrus.database.common.thread.ThreadPoolFactory;
import com.authrus.database.engine.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogArchiver {   

   private static final Logger LOG = LoggerFactory.getLogger(CatalogArchiver.class);
   
   private final ArchivePersister persister;
   private final CatalogExporter exporter;
   private final ThreadFactory factory;
   private final Catalog catalog;
   
   public CatalogArchiver(CatalogExporter exporter, Catalog catalog, File root) {
      this(exporter, catalog, root, 60 * 60 * 1000, 24 * 60 * 60 * 1000);
   }   
   
   public CatalogArchiver(CatalogExporter exporter, Catalog catalog, File root, long frequency) {
      this(exporter, catalog, root, frequency, 24 * 60 * 60 * 1000);
   }
   
   public CatalogArchiver(CatalogExporter exporter, Catalog catalog, File root, long frequency, long expire) {
      this.factory = new ThreadPoolFactory(ArchivePersister.class);
      this.persister = new ArchivePersister(root, frequency, expire);
      this.exporter = exporter;
      this.catalog = catalog;
   }
   
   public void start() {
      if (!persister.isAlive()) {
         Thread thread = factory.newThread(persister);
         
         persister.start();
         thread.start();
      }
   }

   public void stop()  {
      if (persister.isAlive()) {
         persister.stop();
      }
   }
   
   private class ArchivePersister implements Runnable {
      
      private final AtomicBoolean alive;
      private final File root;
      private final long expiry;
      private final long frequency;

      public ArchivePersister(File root, long frequency, long expiry) {
         this.alive = new AtomicBoolean();
         this.frequency = frequency;
         this.expiry = expiry;
         this.root = root;
      }

      public boolean isAlive() {
         return alive.get();
      }
      
      public void start() {
         alive.set(true);
      }

      public void stop() {
         alive.set(false);
      }
      
      public void run() {
         try {
            while(alive.get()) {
               pause();
               clear();
               export();
            }
         } finally {
            alive.set(false);
         }
      }
      
      private void pause() {
         try {
            Thread.sleep(frequency + 1); // never use 0
         } catch(Exception e) {
            LOG.info("Could not pause connector", e);
         }
      }
      
      private void clear() {
         try {
            File[] files = root.listFiles();
            
            if(files != null) {
               long time = System.currentTimeMillis();
               
               for(File file : files) {
                  String name = file.getName();
                  
                  if(name.endsWith(".csv.gz")) {
                     long modified = file.lastModified();
                     long age = time - modified;
                     
                     if(age > expiry) {
                        file.delete();
                     }
                  }
               }
            }
         } catch(Exception e) {
            LOG.info("Could not clear path " + root, e);
         }
      }      
      
      private void export() {
         try {
            if(!root.exists()) {
               root.mkdirs();
            }
            exporter.export(catalog, root);
         } catch(Exception e) {
            LOG.info("Could not export to " + root, e);
         }
      }
   }
}
