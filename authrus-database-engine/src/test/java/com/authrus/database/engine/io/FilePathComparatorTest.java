package com.authrus.database.engine.io;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePathComparator;

import junit.framework.TestCase;

public class FilePathComparatorTest extends TestCase {
   
   public void testOldestFirst() {
      FilePathComparator comparator = new FilePathComparator();
      File directory = new File("c:\\temp");
      FilePath path1 = new FilePath(new File(directory, "jimbo.a"), "jimbo", 1);
      FilePath path2 = new FilePath(new File(directory, "jimbo.b"), "jimbo", 2);
      FilePath path3 = new FilePath(new File(directory, "jimbo.c"), "jimbo", 3);
      List<FilePath> paths = new ArrayList<FilePath>();
      
      paths.add(path2);
      paths.add(path1);
      paths.add(path3);      
      Collections.sort(paths, comparator);
      
      assertEquals(paths.get(0).getTime(), 1); // older files have smaller numbers e.g timestamp
      assertEquals(paths.get(1).getTime(), 2);
      assertEquals(paths.get(2).getTime(), 3);
   }

   public void testNewestFirst() {
      FilePathComparator comparator = new FilePathComparator(false);
      File directory = new File("c:\\temp");
      FilePath path1 = new FilePath(new File(directory, "jimbo.a"), "jimbo", 1);
      FilePath path2 = new FilePath(new File(directory, "jimbo.b"), "jimbo", 2);
      FilePath path3 = new FilePath(new File(directory, "jimbo.c"), "jimbo", 3);
      List<FilePath> paths = new ArrayList<FilePath>();
      
      paths.add(path2);
      paths.add(path1);
      paths.add(path3);      
      Collections.sort(paths, comparator);
      
      assertEquals(paths.get(0).getTime(), 3); // newer files have bigger numbers
      assertEquals(paths.get(1).getTime(), 2);
      assertEquals(paths.get(2).getTime(), 1);
   }
}
