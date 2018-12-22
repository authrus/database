package com.authrus.database.engine;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.util.Random;

public class CreateTestData {
   public static void main(String[] list) throws Exception {
      FileOutputStream out =new FileOutputStream("c:\\Temp\\test.sql");
      PrintStream o = new PrintStream(out);
      Random random = new SecureRandom();
      for(int i = 0; i < 100000; i++) {
         int rand1 = random.nextInt(50000);
         int rand2 = random.nextInt(50000);
         int rand3 = random.nextInt(50000);
         o.println("insert into test (id, name, address, age) values ("+i+", 'name-"+rand1+"', 'address-"+rand2+"', "+rand3+")");
         o.println("go");
      }
      o.close();
   }
}
