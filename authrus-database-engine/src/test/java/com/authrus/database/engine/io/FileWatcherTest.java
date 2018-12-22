package com.authrus.database.engine.io;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePathBuilder;
import com.authrus.database.engine.io.FileWatcher;

import junit.framework.TestCase;

public class FileWatcherTest extends TestCase {
   
   public void testWatcher() throws Exception {
      String tempDirectory = System.getProperty("java.io.tmpdir");
      String name = FileWatcherTest.class.getSimpleName()+System.currentTimeMillis();
      String testDir = new File(tempDirectory, name).getAbsolutePath();      
      FileWatcher watcher = new FileWatcher(testDir);
      
      watcher.start();
      Thread.sleep(1000);
      
      File subDir = new File(testDir, "child1");
      FilePathBuilder subDirBuilder = new FilePathBuilder(testDir, "child1");
      
      subDir.mkdirs();
      
      FilePath newPath = subDirBuilder.createFile();
      File newFile = newPath.getFile();
      FileOutputStream output = new FileOutputStream(newFile);
      
      output.write("test".getBytes("UTF-8"));
      output.flush();
      output.close();      
      Thread.sleep(1000);
      FilePath newPath2 = subDirBuilder.createFile();
      File newFile2 = newPath2.getFile();
      FileOutputStream output2 = new FileOutputStream(newFile2);
      
      output2.write("test".getBytes("UTF-8"));
      output2.flush();
      output2.close();
      
      Thread.sleep(5000);
      
      Map<String, FilePath> files = new HashMap<String, FilePath>();
     
      while(true){
         FilePath path = watcher.next(1000);
         
         if(path == null){
            break;
         }
         System.err.println(path);         
         files.put(path.getFile().getCanonicalPath(), path);
      }
      assertTrue(files.containsKey(newFile.getCanonicalPath()));
      assertTrue(files.containsKey(newFile2.getCanonicalPath()));
   }

}
