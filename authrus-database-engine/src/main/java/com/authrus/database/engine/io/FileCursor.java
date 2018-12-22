package com.authrus.database.engine.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.authrus.database.common.collection.LeastRecentlyUsedMap;

public class FileCursor {

   private final Map<String, FilePath> paths;
   private final Map<String, Long> positions;   
   private final FilePathComparator comparator;
   private final byte[] chunk;

   public FileCursor() {
      this(8192);
   }
   
   public FileCursor(int chunk) {
      this(chunk, 200);
   }
   
   public FileCursor(int chunk, int capacity) {
      this.positions = new LeastRecentlyUsedMap<String, Long>(capacity);
      this.paths = new LinkedHashMap<String, FilePath>();
      this.comparator = new FilePathComparator();
      this.chunk = new byte[chunk];
   }

   public synchronized List<DataBlock> readBlocks(FilePath path) throws IOException {
      String name = path.getName();
      File file = path.getFile();
      FilePath previous = paths.put(name, path);
      
      if(!file.exists()) {
         throw new IOException("File " + file + " is missing");
      }
      List<DataBlock> blocks = new ArrayList<DataBlock>();

      if(previous != null) {
         int comparison = comparator.compare(previous, path);
            
         if(comparison < 0) {
            DataBlock remainder = readBlock(previous); // make sure we finish the file               
           
            if(remainder != null) {
               blocks.add(remainder);  
            }
         }
      }      
      DataBlock next = readBlock(path);
      
      if(next != null) {
         blocks.add(next);
      }
      return blocks;  
   }
   
   private synchronized DataBlock readBlock(FilePath path) throws IOException { 
      RandomAccessFile source = readFile(path);
      
      try {
         File file = path.getFile();
         String name = file.getCanonicalPath();
         long offset = source.getFilePointer();
         long length = source.length();
         long require = length - offset;
         
         if(require > chunk.length) {            
            positions.put(name, length); // update the seek position               
            return new FileRegion(path, offset, (int)require);
         }
         int count = source.read(chunk);
         
         if(count == -1) {
            return null;
         }
         if(count < require) {
            throw new IOException("Could only read " + count +" of " + require + " from " + path);  
         }     
         positions.put(name, offset + count); // update the seek position            
         return new FileBlock(path, chunk, 0, count);         
      } finally {
         source.close();
      } 
   }
   
   private synchronized RandomAccessFile readFile(FilePath path) throws IOException {
      File file = path.getFile();
      
      if(!file.exists()) {
         throw new FileNotFoundException("File " + file + " does not exist");
      }
      RandomAccessFile source = new RandomAccessFile(file, "r");
      
      try {
         String name = file.getCanonicalPath();
         Long seek = positions.get(name);         
         
         if(seek == null) {
            positions.put(name, 0L);
            seek = 0L;
         } else {
            source.seek(seek);
         }     
      } catch(Exception e) {
         throw new IOException("Could not adjust seek position for " + file);  
      }
      return source;
   }
}
