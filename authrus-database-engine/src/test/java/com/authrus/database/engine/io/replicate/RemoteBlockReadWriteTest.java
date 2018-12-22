package com.authrus.database.engine.io.replicate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

import com.authrus.database.engine.io.DataBlock;
import com.authrus.database.engine.io.replicate.RemoteBlock;
import com.authrus.database.engine.io.replicate.RemoteBlockReader;
import com.authrus.database.engine.io.replicate.RemoteBlockWriter;
import com.authrus.database.engine.io.replicate.RemoteConnection;

public class RemoteBlockReadWriteTest extends TestCase {
   
   public void testReadWriteBlock() throws Exception {
      final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] data = "Hello World!!".getBytes();
      RemoteBlock block = new RemoteBlock("test", 11, data, 0, data.length);     
      RemoteBlockWriter writer = new RemoteBlockWriter(new RemoteConnection() {

         @Override
         public DataOutputStream getOutputStream() {
            return new DataOutputStream(buffer);
         }

         @Override
         public DataInputStream getInputStream() {
            return null;
         }         
      });
      writer.writeBlock(block);
      byte[] result = buffer.toByteArray();
      final ByteArrayInputStream source = new ByteArrayInputStream(result);
      RemoteBlockReader reader = new RemoteBlockReader(new RemoteConnection() {

         @Override
         public DataOutputStream getOutputStream() {
            return null;
         }

         @Override
         public DataInputStream getInputStream() {
            return new DataInputStream(source);
         }
         
      });     
      DataBlock restored = reader.readBlock();
      
      assertEquals(restored.getName(), "test");
      assertEquals(restored.getTime(), 11L);
      assertEquals(new String(restored.getData(), "UTF-8"), "Hello World!!");
      assertEquals(restored.getLength(), data.length);
      assertEquals(restored.getOffset(), 0);
      
      
   }

}
