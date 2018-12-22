package com.authrus.database.engine.predicate;

import com.authrus.database.engine.Row;

public class Predicate {
   
   public boolean accept(Row tuple) {
      return true;
   }
}
