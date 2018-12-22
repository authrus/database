package com.authrus.database.engine.index;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import com.authrus.database.Column;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.index.UniqueCluster;

public class UniqueClusterTest extends TestCase {
   
   public void testUniqueCluster() throws Exception {
      Map binding = new HashMap(); // this is used to determine uniqueness across many clusters
      UniqueCluster clusterX = new UniqueCluster(binding, "x", 0);
      UniqueCluster clusterY = new UniqueCluster(binding, "y", 0);
      
      clusterX.insert("x", new Row("",
            new Cell[]{
               new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "name", "name"), "John Doe"),
               new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "address", "address"), "512 Some Address"),
            }
      ));
      clusterX.insert("x", new Row("",
            new Cell[]{
               new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "name", "name"), "John Doe"),
               new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "address", "address"), "512 Some Address"),
            }
      ));
      boolean failure = false;
      
      try{
         clusterY.insert("y", new Row("",
               new Cell[]{
                  new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "name", "name"), "John Doe"),
                  new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "address", "address"), "512 Some Address"),
               }
            ));
      }catch(Exception e) {
         e.printStackTrace();
         failure = true;
      }
      assertTrue("Duplicate was inserted and it should have failed", failure);
      
      failure = false;
      
      try{
         clusterX.remove("x");
         clusterY.insert("y", new Row("",
               new Cell[]{
                  new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "name", "name"), "John Doe"),
                  new Cell(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "address", "address"), "512 Some Address"),
               }
            ));
      }catch(Exception e) {
         e.printStackTrace();
         failure = true;
      }
      assertFalse("Duplicate was removed and should have succeeded", failure);
   }

}
