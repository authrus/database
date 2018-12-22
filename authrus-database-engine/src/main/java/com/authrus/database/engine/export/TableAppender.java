package com.authrus.database.engine.export;

import java.io.IOException;
import java.util.List;

import com.authrus.database.Schema;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.Table;
import com.authrus.database.engine.TableModel;

public class TableAppender {

   private final SchemaAppender schemaAppender;
   private final RowAppender rowAppender;
   private final Appendable appender;
   
   public TableAppender(ValueEscaper escaper, Appendable appender) {
      this.schemaAppender = new SchemaAppender(appender);
      this.rowAppender = new RowAppender(escaper, appender);
      this.appender = appender;
   }
   
   public void append(Table table) throws IOException {
      Schema schema = table.getSchema();
      TableModel model = table.getModel();
      List<Row> rows = model.list();
      
      schemaAppender.append(schema);
      appender.append('\n');
      
      for(Row row : rows) {
         rowAppender.append(row);
         appender.append('\n');
      }
   }
}
