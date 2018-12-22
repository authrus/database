package com.authrus.database.engine.index;

import static com.authrus.database.data.DataConstraint.OPTIONAL;
import static com.authrus.database.data.DataConstraint.UNIQUE;

import java.util.HashMap;
import java.util.Map;

import com.authrus.database.Column;
import com.authrus.database.data.DataConstraint;

public class ClusterBuilder {
   
   private Map<String, Map> bindings;
   
   public ClusterBuilder() {
      this.bindings = new HashMap<String, Map>();
   }
   
   public Cluster create(String key, Column column) {
      DataConstraint constraint = column.getDataConstraint();
      String name = column.getName();
      int index = column.getIndex();

      if(constraint == OPTIONAL) {
         throw new IllegalArgumentException("Unable to index optional column '" + name + "'");
      }     
      if(constraint == UNIQUE) {
         Map binding = bindings.get(name);
         
         if(binding != null) {
            binding = new HashMap();
            bindings.put(name, binding);
         }
         return new UniqueCluster(binding, key, index);         
      }
      return new DuplicateCluster(name, index);
   }
}
