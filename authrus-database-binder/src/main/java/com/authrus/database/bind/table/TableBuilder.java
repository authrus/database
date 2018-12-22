package com.authrus.database.bind.table;

import java.util.List;

import com.authrus.database.Column;
import com.authrus.database.bind.TableBinder;

public interface TableBuilder {
   TableBinder createTable(String name, Class type, String key);
   TableBinder createTable(String name, Class type, List<String> key);
   TableBinder createTable(String name, Class type, String key, List<Column> columns);   
   TableBinder createTable(String name, Class type, List<String> key, List<Column> columns);
}
