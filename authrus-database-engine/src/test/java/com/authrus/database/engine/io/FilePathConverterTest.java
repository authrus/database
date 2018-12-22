package com.authrus.database.engine.io;

import java.io.File;

import com.authrus.database.engine.io.FilePathConverter;

import junit.framework.TestCase;

public class FilePathConverterTest extends TestCase {
   
   public void testCase() throws Exception {
      String tempDir = System.getProperty("java.io.tmpdir");
      String name = FilePathConverterTest.class.getSimpleName()+System.currentTimeMillis();
      FilePathConverter filter = new FilePathConverter();
      File parent = new File(tempDir, name);
      
      assertNotNull(filter.convert(parent, "test.20150303121020000"));
      assertEquals(filter.convert(parent,  "test.20150303121022000").getName(), "test");
      assertEquals(filter.convert(parent,  "test.20150303121022000").getFile().getCanonicalPath(), parent.getCanonicalPath()+"\\test.20150303121022000");
   }

}
