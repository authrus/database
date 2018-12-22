package com.authrus.database.bind.table.attribute;

import static com.authrus.database.data.DataType.TEXT;

import java.util.List;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.function.DefaultValue;

public class ColumnMerger {
   
   private final DataType basic;
   
   public ColumnMerger() {
      this(TEXT);
   }
   
   public ColumnMerger(DataType basic) {
      this.basic = basic;
   }

   public void merge(ColumnSeries series, Column column) {
      String title = column.getTitle();
      List<String> columns = series.getColumns();
      
      if(columns.contains(title)) {
         Column existing = series.getColumn(title);   
         
         if(existing != null) {
            DefaultValue value = existing.getDefaultValue();
            DataConstraint constraint = existing.getDataConstraint();
            DataType override = column.getDataType();
            DataType current = existing.getDataType();
            String expression = value.getExpression();
            String name = column.getName();
            int index = existing.getIndex();
            
            if(current == basic) {
               column = new Column(constraint, override, expression, name, title, index);
            } else {
               column = new Column(constraint, current, expression, name, title, index);
            }
         }
      }
      series.addColumn(column);
   }
}
