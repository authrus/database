package com.authrus.database.engine.io;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

public class FileAppender {
 
   private final AtomicReference<FileChannel> reference;
   private final FilePointer pointer;
   private final FilePath path;
   
   public FileAppender(FilePointer pointer, FilePath path) {
      this.reference = new AtomicReference<FileChannel>();
      this.pointer = pointer;
      this.path = path;
   }
   
   public int append(byte[] array) throws IOException {
      return append(array, 0, array.length);
   }
   
   public int append(byte[] array, int off, int length) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(array, off, length);        
      FileChannel channel = reference.get();
      File file = path.getFile();
      Path path = file.toPath(); 
      
      if(channel == null) {
         channel = FileChannel.open(path, CREATE, WRITE, APPEND);
         reference.set(channel);
      }
      return channel.write(buffer);
   }
   
   public boolean previous() throws IOException {
      FilePath previous = pointer.previous();
      File expect = previous.getFile();
      File actual = path.getFile();
   
      return actual.equals(expect);
   }
   
   public boolean current() throws IOException {
      FilePath current = pointer.current();
      File expect = current.getFile();
      File actual = path.getFile();
   
      return actual.equals(expect);
   }
   
   public boolean next() throws IOException {
      FilePath next = pointer.next();
      File expect = next.getFile();
      File actual = path.getFile();
   
      return actual.equals(expect);
   }
   
   public boolean exists() throws IOException {
      File file = path.getFile();
      
      if(file.exists()) {
         return true;
      }
      return false;
   }
   
   public long length() throws IOException {
      File file = path.getFile();
      
      if(file.exists()) {
         return file.length();
      }
      return 0;
   }   
   
   public void close() throws IOException {
      FileChannel channel = reference.get();
      
      if(channel != null) {
         channel.close();    
         reference.set(null);
      }
   }
}