package com.authrus.database.common.io;

import java.io.IOException;

public interface DataWriter {
   void writeInt(int value) throws IOException;
   void writeLong(long value) throws IOException;
   void writeByte(byte value) throws IOException;   
   void writeShort(short value) throws IOException;
   void writeString(String value) throws IOException;
   void writeChar(char value) throws IOException;
   void writeBoolean(boolean value) throws IOException;
   void writeFloat(float value) throws IOException;
   void writeDouble(double value) throws IOException;
}
