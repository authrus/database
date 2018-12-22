package com.authrus.database.engine;

import java.io.IOException;

import junit.framework.TestCase;

import com.authrus.database.Column;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.TableState;

public class TableStateTest extends TestCase {
   
   public void testState() throws IOException {
      TableState state = new TableState("test");
      
      for(int i = 0; i < 10000; i++) {
         Cell[] cells = new Cell[]{
               new Cell(new Column(DataConstraint.REQUIRED, DataType.INT, null, "id", "id", 0), i),
               new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "name", "name", 1), "name-"+i),
               new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "address", "address", 2), "address-"+i)};
         
         Row tuple = new Row(String.valueOf(i), cells);
         state.insert(String.valueOf(i), tuple);
         
         if(i == 1567) {
            state.mark();
         }
      }
      for(int i = 0; i < 10000; i++) {
         assertEquals(state.get(String.valueOf(i)).getCell(1).getValue(), "name-"+i);
         assertEquals(state.get(String.valueOf(i)).getCell(2).getValue(), "address-"+i);
      }
      assertTrue(state.revert());
      assertFalse(state.revert());
      
      for(int i = 0; i < 10000; i++) {
         if(i <= 1567) {
            assertEquals(state.get(String.valueOf(i)).getCell(1).getValue(), "name-"+i);
            assertEquals(state.get(String.valueOf(i)).getCell(2).getValue(), "address-"+i);
         } else {
            assertNull(state.get(String.valueOf(i)));
         }
      }
   }

}
