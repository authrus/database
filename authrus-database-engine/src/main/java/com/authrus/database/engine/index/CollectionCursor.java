package com.authrus.database.engine.index;

import java.util.Collection;
import java.util.Iterator;

import com.authrus.database.engine.Row;

public class CollectionCursor implements RowCursor {
   
   private final Collection<Row> tuples;
   
   public CollectionCursor(Collection<Row> tuples) {
      this.tuples = tuples;
   }
   
   public Iterator<Row> iterator() {
      return tuples.iterator();
   }

   @Override
   public int count() {
      return tuples.size();
   }

}
