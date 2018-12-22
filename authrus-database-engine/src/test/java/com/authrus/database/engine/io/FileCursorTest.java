package com.authrus.database.engine.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.util.List;

import com.authrus.database.engine.io.DataBlock;
import com.authrus.database.engine.io.FileCursor;
import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePathBuilder;

import junit.framework.TestCase;

public class FileCursorTest extends TestCase {
   
   public String createTempDir(String name) {
      String directory = System.getProperty("java.io.tmpdir"); 
      File newDir = new File(directory, name);
      if(!newDir.exists()) {
         newDir.mkdirs();
      }
      return newDir.getAbsolutePath();      
   }
   
   public void testUTF() throws Exception {
      String directory = createTempDir("testUTF");
      FileCursor cursor = new FileCursor();
      FilePathBuilder builder = new FilePathBuilder(directory, "test");
      FilePath path = builder.createFile();
      File file = path.getFile();
      FileOutputStream out = new FileOutputStream(file);
      OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
      
      writer.write("Hello World\r\n");
      writer.write("Second line\r\n");
      writer.write("Blah End!!\r\n");
      writer.flush();
      
      List<DataBlock> blocks = cursor.readBlocks(path);
      DataBlock block = blocks.get(0);
      byte[] data = block.getData();
      int length = block.getLength();
      int offset = block.getOffset();     
      
      ByteArrayInputStream in = new ByteArrayInputStream(data, offset, length);
      InputStreamReader reader = new InputStreamReader(in, "UTF-8");
      LineNumberReader iterator = new LineNumberReader(reader);      
      
      assertEquals(iterator.readLine(), "Hello World");
      assertEquals(iterator.readLine(), "Second line");
      assertEquals(iterator.readLine(), "Blah End!!");
      assertNull(iterator.readLine());
      
      writer.close();
   }
   
   public void testFileCursor() throws Exception {
      String directory = createTempDir("testFileCursor");
      FileCursor cursor = new FileCursor();
      FilePathBuilder builder = new FilePathBuilder(directory, "test");
      FilePath path = builder.createFile();
      File file = path.getFile();
      FileOutputStream out = new FileOutputStream(file);
      DataOutputStream s = new DataOutputStream(out);
      
      s.writeInt(1);
      s.writeLong(22L);
      s.writeUTF("test");
      s.writeBoolean(true);
      s.flush();
      
      List<DataBlock> blocks = cursor.readBlocks(path);
      DataBlock block = blocks.get(0);
      byte[] data = block.getData();
      int length = block.getLength();
      int offset = block.getOffset();     
      
      ByteArrayInputStream in = new ByteArrayInputStream(data, offset, length);
      DataInputStream i = new DataInputStream(in);      
      
      assertEquals(i.readInt(), 1);
      assertEquals(i.readLong(), 22L);
      assertEquals(i.readUTF(), "test");
      assertEquals(i.readBoolean(), true);
      assertEquals(in.read(), -1);
      
      s.writeInt(23);
      s.writeLong(1112222L);
      s.writeUTF("another test");
      s.writeBoolean(false);
      
      List<DataBlock> blocks2 = cursor.readBlocks(path);
      DataBlock block2 = blocks2.get(0);
      byte[] data2 = block2.getData();
      int length2 = block2.getLength();
      int offset2 = block2.getOffset();     
      
      ByteArrayInputStream in2 = new ByteArrayInputStream(data2, offset2, length2);
      DataInputStream i2 = new DataInputStream(in2);      
      
      assertEquals(i2.readInt(), 23);
      assertEquals(i2.readLong(), 1112222L);
      assertEquals(i2.readUTF(), "another test");
      assertEquals(i2.readBoolean(), false);
      //assertEquals(i2.read(), -1);
      
      s.close();
   }
}
