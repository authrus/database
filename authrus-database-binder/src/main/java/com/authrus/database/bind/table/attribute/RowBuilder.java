package com.authrus.database.bind.table.attribute;

import com.authrus.database.attribute.ObjectBuilder;
import com.authrus.database.attribute.ReflectionBuilder;
import com.authrus.database.attribute.SerializationBuilder;
import com.authrus.database.attribute.SerializationStreamBuilder;

public class RowBuilder implements ObjectBuilder {
   
   private final ObjectBuilder[] builders;
   
   public RowBuilder() {
      this.builders = new ObjectBuilder[3];
      this.builders[0] = new ReflectionBuilder();
      this.builders[1] = new SerializationBuilder();
      this.builders[2] = new SerializationStreamBuilder();
   }
  
   @Override
   public Object createInstance(Class type) {
      for (ObjectBuilder factory : builders) {
         Object value = factory.createInstance(type);

         if (value != null) {
            return value;
         }
      }
      return null;
   }

}
