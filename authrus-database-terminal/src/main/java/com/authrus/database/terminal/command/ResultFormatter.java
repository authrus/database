package com.authrus.database.terminal.command;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import com.authrus.database.Column;
import com.authrus.database.Record;
import com.authrus.database.Schema;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;
import com.authrus.database.terminal.session.SessionContext;
import com.authrus.database.terminal.session.SessionTime;

public class ResultFormatter {
   
   private static final String DATE_FORMAT = "dd-MM-yyyy HH:mm:ss.SSS z";
   
   private final QueryRequest request;
   private final SessionContext session;
   
   public ResultFormatter(SessionContext session, QueryRequest request) {
      this.session = session;
      this.request = request;
   }   
   
   public Map<String, String> format(Record record) throws Exception {
      Catalog catalog = session.getCatalog();
      Query query = request.getQuery();
      String name = query.getTable();
      Table table = catalog.findTable(name);
      Schema schema = table.getSchema();
      Verb verb = query.getVerb();
      
      if(verb == Verb.SELECT || verb == Verb.SELECT_DISTINCT) {
         List<String> layout = schema.getColumns();
         Set<String> titles = record.getColumns();
         SessionTime time = session.getTime();
         TimeZone zone = time.getZone();         
         
         if(!titles.isEmpty()) {
            Map<String, String> values = new LinkedHashMap<String, String>();
            DateFormat format = new SimpleDateFormat(DATE_FORMAT);
            
            format.setTimeZone(zone);
            
            for(String title : titles) {
               String value = record.getString(title);
            
               if(layout.contains(title)) {            
                  Column column = schema.getColumn(title);
                  DataType type = column.getDataType();
                  
                  if(type == DataType.DATE) {
                     Long number = Long.parseLong(value);
                     String date = format.format(number);
                     
                     values.put(title, date);
                  } else {
                     values.put(title, value);
                  }
               } else{
                  values.put(title, value);
               }
            }
            return values;
         }
      }
      return Collections.emptyMap();
   }
}
