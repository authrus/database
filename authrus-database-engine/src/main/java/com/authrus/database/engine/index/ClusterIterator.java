package com.authrus.database.engine.index;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.authrus.database.engine.Row;

public class ClusterIterator implements Iterator<Row> {
   
   private Iterator<Cluster> clusters;
   private Iterator<Row> tuples;
   private Set<Cluster> exclude;
   private Row next;

   public ClusterIterator(Iterator<Cluster> clusters) {
      this(clusters, Collections.EMPTY_SET);
   }
   
   public ClusterIterator(Iterator<Cluster> clusters, Set<Cluster> exclude) {      
      this.clusters = clusters;
      this.exclude = exclude;
   }

   @Override
   public boolean hasNext() {
      if(next == null) {
         if(tuples == null) { // get first iterator
            return moveNext();
         } else {
            if(tuples.hasNext()) { 
               next = tuples.next();
            } else {
               return moveNext();            
            }
         }
      }
      return true;
   }

   private boolean moveNext() {
      while(clusters.hasNext()) {
         Cluster cluster = clusters.next();
         
         if(!exclude.contains(cluster)) {
            Iterator<Row> iterator = cluster.iterator();
            
            if(iterator.hasNext()) {
               next = iterator.next();
               tuples = iterator;
               return true;
            }
         }
      }
      return false;
   }
   
   @Override
   public Row next() {
      Row result = next;
      
      if(result == null) {
         if(hasNext()) {
            result = next;
         }
      }
      if(result != null) {
         next = null;
      }
      return result;      
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
   }

}
