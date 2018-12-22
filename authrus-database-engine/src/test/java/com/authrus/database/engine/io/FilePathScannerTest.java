package com.authrus.database.engine.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePathBuilder;
import com.authrus.database.engine.io.FilePathScanner;

import junit.framework.TestCase;

public class FilePathScannerTest extends TestCase {
   
   public void testPath() throws IOException {
      String tempDir = System.getProperty("java.io.tmpdir");
      String name = FilePathScannerTest.class.getSimpleName()+System.currentTimeMillis();
      FilePathScanner scanner = new FilePathScanner(tempDir);
      FilePathBuilder builder = new FilePathBuilder(tempDir, name);
      FilePath path =builder.createFile();
      File file = path.getFile();
      System.err.println("Created file " + file);
      FileOutputStream out = new FileOutputStream(file);
      out.write("test".getBytes("UTF-8"));
      out.close();
      List<FilePath> list = scanner.listFiles();
      for(FilePath entry : list){
         System.err.println(entry);
      }
   }

}
