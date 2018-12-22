package com.authrus.database.bind.table.attribute;

import java.lang.reflect.Field;

import com.authrus.database.Column;

public interface Structure {
   Structure addChild(Field field);
   void addColumn(Column column);
   void addColumn(Field field);   
}
