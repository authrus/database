package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.CREATE;

import java.io.IOException;
import java.util.List;

import com.authrus.database.Column;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;
import com.authrus.database.function.DefaultValue;

public class CreateRecordWriter implements ChangeRecordWriter {
   
   private final Schema schema;  
   private final String origin;
   
   public CreateRecordWriter(String origin, Schema schema) {
      this.origin = origin;
      this.schema = schema;
   }

   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {
      if(schema == null) {
         throw new IllegalStateException("Create does not have a schema");
      }
      PrimaryKey key = schema.getKey();
      List<String> keys = key.getColumns();
      int count = schema.getCount();
      
      if(count <= 0) {
         throw new IllegalStateException("Schema does not have any columns");
      }
      writer.writeChar(CREATE.code);
      writer.writeString(origin);
      writer.writeInt(count);
      
      for(int i = 0; i < count; i++) {
         Column column = schema.getColumn(i);
         String name = column.getName();
         DataType type = column.getDataType();
         DataConstraint constraint = column.getDataConstraint();
         DefaultValue value = column.getDefaultValue();
         String expression = value.getExpression();
         
         if(type == null) {
            throw new IllegalStateException("Schema has no type for column '" + name + "'");
         }
         if(constraint == null) {
            throw new IllegalStateException("Schema has no constraint for column '" + name + "'");
         }
         writer.writeString(name);
         writer.writeString(expression);
         writer.writeChar(type.code);
         writer.writeChar(constraint.code);
         
         if(keys.contains(name)) {
            writer.writeBoolean(true);
         } else {
            writer.writeBoolean(false);
         }
      }
   }
}
