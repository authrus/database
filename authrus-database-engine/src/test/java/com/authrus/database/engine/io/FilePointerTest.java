package com.authrus.database.engine.io;

import java.io.FileOutputStream;

import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePathBuilder;
import com.authrus.database.engine.io.FilePointer;

import junit.framework.TestCase;

public class FilePointerTest extends TestCase {   
   
   public void testPointer() throws Exception {
      String tempDir = System.getProperty("java.io.tmpdir");
      String name = FilePointerTest.class.getSimpleName()+System.currentTimeMillis();
      FilePointer pointer = new FilePointer(tempDir, name);
      FilePathBuilder builder = new FilePathBuilder(tempDir, name);
      
      FilePath entry1 = builder.createFile();
      Thread.sleep(10);
      FilePath entry2 = builder.createFile();
      Thread.sleep(10);
      FilePath entry3 = builder.createFile();
      Thread.sleep(10);
      FilePath entry4 = builder.createFile();
      
      createFile(entry1);
      createFile(entry2);
      createFile(entry3);
      createFile(entry4);
      
      assertNull(pointer.previous()); 
      assertEquals(pointer.current().getFile(), entry1.getFile());
      assertEquals(pointer.current().getFile(), entry1.getFile());
      assertEquals(pointer.next().getFile(), entry2.getFile());
      assertEquals(pointer.next().getFile(), entry3.getFile());
      assertEquals(pointer.current().getFile(), entry3.getFile());
      assertEquals(pointer.previous().getFile(), entry2.getFile());
      assertEquals(pointer.previous().getFile(), entry1.getFile());
      assertNull(pointer.previous());  
      assertEquals(pointer.current().getFile(), entry1.getFile());
      assertNull(pointer.previous()); 
      
   }
   public void createFile(FilePath p)throws Exception{
      FileOutputStream o = new FileOutputStream(p.getFile());
      o.write(p.getFile().getCanonicalPath().getBytes("UTF-8"));
      o.close();
   }

}
