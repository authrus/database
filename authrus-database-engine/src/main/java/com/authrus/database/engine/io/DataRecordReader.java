package com.authrus.database.engine.io;

import java.io.IOException;

import com.authrus.database.common.io.DataFormatter;
import com.authrus.database.common.io.DataReader;

public class DataRecordReader implements DataReader {
   
   private final DataFormatter formatter;
   private final DataReader reader;
   
   public DataRecordReader(DataReader reader) {
      this.formatter = new DataFormatter();
      this.reader = reader;
   }
   
   public Comparable readValue() throws IOException {
      return (Comparable)formatter.read(reader);
   }

   @Override
   public String readString() throws IOException {
      return (String)formatter.read(reader);
   }

   @Override
   public int readInt() throws IOException {
      return reader.readInt();
   }

   @Override
   public long readLong() throws IOException {
      return reader.readLong();
   }

   @Override
   public byte readByte() throws IOException {
      return reader.readByte();
   }

   @Override
   public short readShort() throws IOException {
      return reader.readShort();
   }

   @Override
   public char readChar() throws IOException {
      return reader.readChar();
   }

   @Override
   public boolean readBoolean() throws IOException {
      return reader.readBoolean();
   }

   @Override
   public float readFloat() throws IOException {
      return reader.readFloat();
   }

   @Override
   public double readDouble() throws IOException {
      return reader.readDouble();
   }

}
