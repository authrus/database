package com.authrus.database.engine.io;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.common.thread.ThreadPoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWatcher {

   private static final Logger LOG = LoggerFactory.getLogger(FileWatcher.class);

   private final BlockingQueue<FilePath> changes;
   private final DirectoryScanner scanner;
   private final ThreadFactory factory;   
   private final File directory;

   public FileWatcher(String directory)  {
      this.factory = new ThreadPoolFactory(DirectoryScanner.class);
      this.changes = new LinkedBlockingQueue<FilePath>();
      this.scanner = new DirectoryScanner(directory);
      this.directory = new File(directory);
   }

   public FilePath next(long wait) {
      if(!scanner.isAlive()) {
         throw new IllegalStateException("Scanner is not active for '" + directory + "'");
      }
      try {
         return changes.poll(wait, MILLISECONDS);
      } catch (Exception e) {
         throw new IllegalStateException("Error waiting for change to '" + directory + "'");
      }
   }

   public void start() {
      if (!scanner.isAlive()) {
         Thread thread = factory.newThread(scanner);
         
         scanner.start();
         thread.start();        
      }
   }

   public void stop()  {
      if (scanner.isAlive()) {
         scanner.stop();
      }
   }

   private class DirectoryScanner implements Runnable {

      private final AtomicReference<WatchService> reference;
      private final Map<WatchKey, Path> registry;
      private final FilePathConverter converter;
      private final FilePathScanner scanner;
      private final AtomicBoolean alive;

      public DirectoryScanner(String directory) {
         this.reference = new AtomicReference<WatchService>();      
         this.registry = new ConcurrentHashMap<WatchKey, Path>();
         this.scanner = new FilePathScanner(directory);
         this.converter = new FilePathConverter();
         this.alive = new AtomicBoolean();
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

      @Override
      public void run() {
         try { 
            try {
               create();
               scan();
               watch();
            } finally {
               close();
            }
         } catch (Exception e) {
            LOG.info("Error scanning '" + directory + "'", e);
         }
      }
      
      private void create() throws Exception {
         FileSystem system = FileSystems.getDefault();
         WatchService service = system.newWatchService();                     
  
         if(!directory.exists()) {
            directory.mkdirs();
         }
         reference.set(service); 
      }

      private void scan() throws Exception {
         List<FilePath> paths = scanner.listFiles();         
         
         if(register(directory)) {
            LOG.info("Watching directory '" + directory + "' for updates");
         }
         for(FilePath path : paths) {
            File file = path.getFile();
            File parent = file.getParentFile();
            
            if(register(parent)) {
               LOG.info("Watching directory '" + parent + "' for updates");
            }
            changes.offer(path);               
         }            
      }
      
      private boolean register(File file) throws Exception {
         Path path = file.toPath(); 
         WatchService service = reference.get();  
         
         if(!contains(file) && file.exists()) {
            WatchKey key = path.register(service, ENTRY_CREATE, ENTRY_MODIFY);
                        
            if(key.isValid()) {               
               registry.put(key, path);
               return true;
            }
         }
         return false;
      }
      
      private boolean contains(File file) throws Exception {
         Set<WatchKey> keys = registry.keySet();
         Path path = file.toPath();
         
         for(WatchKey key : keys) {
            Path value = registry.get(key);            
               
            if(value.equals(path)) {               
               return true;
            }
         }
         return false;        
      }
      
      private void watch() throws Exception {
         WatchService service = reference.get();
         
         try {
            while (alive.get()) {
               WatchKey key = service.take();
               List<WatchEvent<?>> events = key.pollEvents();
   
               for (WatchEvent<?> event : events) {
                  WatchEvent.Kind<?> kind = event.kind();               
   
                  if (kind != OVERFLOW) {
                     Object context = event.context();
                     
                     if(context != null) {                     
                        Path path = (Path)context;                     
                        Path parent = registry.get(key);
                        Path resolve = parent.resolve(path);
                        File file = resolve.toFile();
                        File directory = parent.toFile();
                        String name = file.getName();                                        
                        
                        if(file.isDirectory()) {
                           if(register(file)) {
                              LOG.info("Watching directory '" + file + "' for updates");
                           }
                        } else {
                           process(directory, name);
                        } 
                     }
                  }
               }
               if (!key.reset()) { // remove if no longer accessible
                  registry.remove(key);
               }
            } 
         } finally {
            alive.set(false);
         }
      }
      
      private void process(File parent, String name) throws Exception {         
         FilePath path = converter.convert(parent, name);
         
         if(path != null) {
            File file = path.getFile();
         
            if(file.exists()) {
               changes.offer(path);
            }
         } else {
            LOG.info("Ignoring update to '" + name + "'");
         }
      }
      
      private void close() throws Exception {        
         WatchService service = reference.get();                     
  
         if(service != null) {
            service.close();
         }
         registry.clear();
         reference.set(null);
      }
   }
}
