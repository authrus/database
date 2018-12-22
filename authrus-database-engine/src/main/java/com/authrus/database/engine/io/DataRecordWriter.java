package com.authrus.database.engine.io;

import java.io.IOException;

import com.authrus.database.common.io.DataFormatter;
import com.authrus.database.common.io.DataWriter;

public class DataRecordWriter implements DataWriter {
   
   private final DataFormatter formatter;
   private final DataWriter writer;
   
   public DataRecordWriter(DataWriter writer) {
      this.formatter = new DataFormatter();
      this.writer = writer;
   }   

   public void writeValue(Object value) throws IOException {
      formatter.write(writer, value);  
   }

   @Override
   public void writeString(String value) throws IOException {
      formatter.write(writer, value);  
   }

   @Override
   public void writeInt(int value) throws IOException {
      writer.writeInt(value);
   }

   @Override
   public void writeLong(long value) throws IOException {
      writer.writeLong(value);
   }

   @Override
   public void writeByte(byte value) throws IOException {
      writer.writeByte(value);
   }

   @Override
   public void writeShort(short value) throws IOException {
      writer.writeShort(value);
   }

   @Override
   public void writeChar(char value) throws IOException {
      writer.writeChar(value);
   }

   @Override
   public void writeBoolean(boolean value) throws IOException {
      writer.writeBoolean(value);
   }

   @Override
   public void writeFloat(float value) throws IOException {
      writer.writeFloat(value);
   }

   @Override
   public void writeDouble(double value) throws IOException {
      writer.writeDouble(value);
   }
}
