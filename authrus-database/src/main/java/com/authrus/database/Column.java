package com.authrus.database;

import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.function.DefaultFunction;
import com.authrus.database.function.DefaultValue;

public class Column {

   private final DataConstraint constraint;
   private final DefaultValue value;
   private final DataType data;
   private final String title;
   private final String name;
   private final int index;

   public Column(DataConstraint constraint, DataType data, String value, String name, String title) {
      this(constraint, data, value, name, title, -1);      
   }
   
   public Column(DataConstraint constraint, DataType data, String value, String name, String title, int index) {
      this.value = DefaultFunction.resolveValue(value);
      this.constraint = constraint;      
      this.index = index;
      this.title = title;
      this.name = name;
      this.data = data;     
   }
   
   public String getTitle() {
      return title;
   }
   
   public String getName() {
      return name;
   }
   
   public DefaultValue getDefaultValue() {
      return value;
   }      
   
   public DataConstraint getDataConstraint() {
      return constraint;
   }     
   
   public DataType getDataType() {
      return data;
   }   
   
   public int getIndex() {
      return index;
   }
   
   @Override
   public String toString() {
      return String.format("%s %s at %s", constraint, title, index);
   }
}
