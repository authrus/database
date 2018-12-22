package com.authrus.database.engine.io.replicate;

import com.authrus.database.engine.io.DataBlock;

public class RemoteBlock implements DataBlock {

   private final String name;
   private final byte[] data;
   private final int offset;
   private final int length;
   private final long time;
   
   public RemoteBlock(String name, long time, byte[] data, int offset, int length) {
      this.length = length;
      this.offset = offset;
      this.name = name;
      this.data = data;
      this.time = time;
   }
   
   @Override
   public String getName() {
      return name;
   }
   
   public long getTime() {
      return time;
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
   public String toString() {
      return String.format("%s -> %s", name, data.length);
   }
}
