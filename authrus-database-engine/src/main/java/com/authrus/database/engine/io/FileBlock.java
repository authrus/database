package com.authrus.database.engine.io;

import java.io.File;

public class FileBlock implements DataBlock {

   private final FilePath path;
   private final byte[] data;
   private final int offset;
   private final int length;
   
   public FileBlock(FilePath path, byte[] data, int offset, int length) {
      this.length = length;
      this.offset = offset;
      this.data = data;
      this.path = path;
   }
   
   public File getFile() {
      return path.getFile();
   }
   
   @Override
   public String getName() {
      return path.getName();
   }
   
   @Override
   public byte[] getData() {
      return data;
   }
   
   @Override
   public int getOffset() {
      return offset;
   }
   
   @Override
   public int getLength() {
      return length;
   }
   
   @Override
   public long getTime() {
      return path.getTime();
   }
   
   @Override
   public String toString() {
      return String.format("%s -> %s", path, data.length);
   }
}
