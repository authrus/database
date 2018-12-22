package com.authrus.database.engine.index;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import com.authrus.database.Column;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.index.KeyIndex;

public class KeyIndexTest extends TestCase {
   
   public void testKeyIndex() throws Exception {
      Column column = new Column(DataConstraint.REQUIRED, DataType.INT, null, "id", "id", 0);
      KeyIndex index = new KeyIndex(column);
      Random random = new SecureRandom();
      List<Integer> numbers = new ArrayList<Integer>();
      
      for(int i = 0; i < 10000; i++) {
         numbers.add(i);
      }
      Collections.shuffle(numbers, random);
      
      for(int i = 0; i < 10000; i++) {
         Integer number = numbers.remove(0);
         String name = "name-"+number;
         String address = "address-"+number;
         
         System.err.println("INSERT: " + number);
         index.update(new Row(String.valueOf(number),
               new Cell[]{
                  new Cell(new Column(DataConstraint.REQUIRED, DataType.INT, null, "id", "id", 0), number),
                  new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "name", "name", 1), name),
                  new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "address", "address", 2), address),
               }
         ));
      }
      Iterator<Row> iterator = index.lessThan(10).iterator();
      int i = 0;
      
      while(iterator.hasNext()) {
         Comparable next = iterator.next().getCell(1).getValue();
         System.err.println("VALUE: "+next);
         assertEquals(next, "name-"+(i++));
      }
      assertEquals(i, 10);
   }

}
