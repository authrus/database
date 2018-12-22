package com.authrus.database.engine.text;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import com.authrus.database.engine.text.LineParser;

public class LineParserTest extends TestCase {

   public void testSplitterOneBlock() throws IOException {
      LineParser splitter = new LineParser();
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(buffer, true, "UTF-8");

      System.out.println("\u5feb\u4e50\u5b66\u4e60");
      System.out.println();

      out.println("\u5feb\u4e50\u5b66\u4e60");
      out.println("This is a simple test");
      out.println("This is a simple test");
      out.println("Blah blah");
      out.println("साहिलसाहिल");
      out.println("Next line");
      out.flush();
      out.close();

      byte[] input = buffer.toByteArray();

      List<String> lines = splitter.update(input);
      for (String line : lines) {
         System.err.println(line);
      }
   }

   public void testSplitter() throws IOException {
      LineParser splitter = new LineParser();
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(buffer, true, "UTF-8");

      System.out.println("This is a simple test");
      System.out.println("साहिलसाहिल");
      System.out.println("Yet another");
      System.out.println("\u8ba9\u4ed6\u51fa\u6d77\u4e86");
      System.out.println("\u5feb\u4e50\u5b66\u4e60");
      System.out.println("Final line");
      System.out.println();

      out.println("This is a simple test");
      out.println("साहिलसाहिल");
      out.println("Yet another");
      out.println("\u8ba9\u4ed6\u51fa\u6d77\u4e86");
      out.println("\u5feb\u4e50\u5b66\u4e60");
      out.println("Final line");
      out.flush();
      out.close();

      byte[] chunk = new byte[1024];
      Random random = new SecureRandom();
      byte[] input = buffer.toByteArray();
      
      for(int i = 0; i < 1000; i++) {
         ByteArrayInputStream source = new ByteArrayInputStream(input);
         List<String> expect = new ArrayList<String>();
         List<String> actual = new ArrayList<String>();
         
         expect.add("This is a simple test");
         expect.add("साहिलसाहिल");
         expect.add("Yet another");
         expect.add("\u8ba9\u4ed6\u51fa\u6d77\u4e86");
         expect.add("\u5feb\u4e50\u5b66\u4e60");
         expect.add("Final line");
         
         while (true) {
            int size = random.nextInt(5) + 1;
            int count = source.read(chunk, 0, size);
   
            if (count == -1) {
               break;
            }
            List<String> lines = splitter.update(chunk, 0, count);
            
            for (String line : lines) {
               actual.add(line);
            }
         }
         assertEquals(actual.size(), expect.size());
         
         for(int j = 0; j < actual.size(); j++) {
            assertEquals(actual.get(j), expect.get(j));
         }
      }
   }

}
