package com.authrus.database.engine.io.read;

import java.io.IOException;
import java.util.Properties;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.write.ChangeRecordReader;

public class CreateRecordReader implements ChangeRecordReader{
   
   private final String origin;
   private final String table;
   
   public CreateRecordReader(String origin, String table) {
      this.origin = origin;
      this.table = table;
   }
   
   @Override
   public ChangeOperation read(DataRecordReader reader) throws IOException {
      int count = reader.readInt();
         
      if(count == 0) {
         throw new IllegalStateException("Create statement for '" + table + "' has no columns");
      }
      ColumnSeries keys = new ColumnSeries();
      ColumnSeries columns = new ColumnSeries();
      PrimaryKey key = new PrimaryKey(keys);
      Properties properties = new Properties();
      Schema schema = new Schema(key, columns, properties);
      
      for(int i = 0; i < count; i++) {
         String name = reader.readString();
         String expression = reader.readString();
         char type = reader.readChar();
         char constraint = reader.readChar();
         
         if(name == null) {
            throw new IllegalStateException("Column name at index " + i + " is null for '" + table + "'");
         }
         DataType data = DataType.resolveType(type);            
         DataConstraint restriction = DataConstraint.resolveConstraint(constraint);
      
         if(name != null) {
            Column column = new Column(restriction, data, expression, name, name, i);
         
            if(reader.readBoolean()) { // is it a key
               keys.addColumn(column);
            }
            columns.addColumn(column);
         }         
      }
      return new CreateOperation(origin, table, schema);
   }
}
