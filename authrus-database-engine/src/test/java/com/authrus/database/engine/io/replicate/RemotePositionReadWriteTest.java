package com.authrus.database.engine.io.replicate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import com.authrus.database.engine.io.replicate.Position;
import com.authrus.database.engine.io.replicate.RemoteConnection;
import com.authrus.database.engine.io.replicate.RemotePositionReader;
import com.authrus.database.engine.io.replicate.RemotePositionWriter;

import junit.framework.TestCase;

public class RemotePositionReadWriteTest extends TestCase {
   
   public void testPositionReadWrite() throws Exception {
      final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      Position pos = new Position();
      pos.setTime("x", 22);
      pos.setTime("y", 12);
      RemotePositionWriter writer = new RemotePositionWriter(new RemoteConnection() {

         @Override
         public DataOutputStream getOutputStream() {
            return new DataOutputStream(buffer);
         }

         @Override
         public DataInputStream getInputStream() {
            return null;
         }         
      });
      writer.writePosition(pos);
      byte[] result = buffer.toByteArray();
      final ByteArrayInputStream source = new ByteArrayInputStream(result);
      RemotePositionReader reader = new RemotePositionReader(new RemoteConnection() {

         @Override
         public DataOutputStream getOutputStream() {
            return null;
         }

         @Override
         public DataInputStream getInputStream() {
            return new DataInputStream(source);
         }
         
      });
      Position restored = new Position();
      reader.readPosition(restored);
      
      assertEquals(restored.getTime("x"), pos.getTime("x"));
      assertEquals(restored.getTime("y"), pos.getTime("y"));
      assertEquals(restored.getTime("z"), pos.getTime("z"));
   }
   

}
