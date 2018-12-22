package com.authrus.database.bind.table.attribute;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.authrus.database.Column;
import com.authrus.database.Schema;
import com.authrus.database.attribute.FieldAccessor;
import com.authrus.database.attribute.ObjectBuilder;
import com.authrus.database.attribute.ObjectNode;
import com.authrus.database.attribute.ObjectScanner;

public class AttributeSchemaScanner {

   private final ObjectScanner scanner; 
   
   public AttributeSchemaScanner(ObjectBuilder builder) {
      this.scanner = new ObjectScanner(builder);
   }
   
   public Schema createSchema(Class type, String name) {
      return createSchema(type, name, Collections.EMPTY_LIST);
   }
   
   public Schema createSchema(Class type, List<String> names) {
      return createSchema(type, names, Collections.EMPTY_LIST);
   }
   
   public Schema createSchema(Class type, String name, List<Column> columns) {
      return createSchema(type, Arrays.asList(name), columns);
   }
   
   public Schema createSchema(Class type, List<String> names, List<Column> columns) {
      StructureBuilder structure = new StructureBuilder(type, names);
      
      if(names.isEmpty()) {
         throw new IllegalStateException("Key must contain at least one column for " + type);
      }
      for(Column column : columns) {
         structure.addColumn(column);
      }
      createSchema(type, structure);

      return structure.createSchema();
   }
   
   private void createSchema(Class schema, Structure structure) {
      ObjectNode node = scanner.createNode(schema);
      List<FieldAccessor> accessors = node.getFields();
      
      for(FieldAccessor accessor : accessors) {
         Field field = accessor.getField();
         Class type = field.getType();
    
         if(accessor.isArray()) {
            throw new IllegalStateException("Arrays cannot be used as columns for " + field);
         } 
         if(accessor.isPrimitive() || accessor.isTransform()) {            
            structure.addColumn(field);
         } else {
            Structure child = structure.addChild(field);
                  
            if(child != null) {
               createSchema(type, child);
            }
         }
      }
   }
}
