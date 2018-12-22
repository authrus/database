package com.authrus.database.terminal.session;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.authrus.database.Database;
import com.authrus.database.engine.Catalog;

public class SessionContext {
   
   private static final String DECIMAL_FORMAT = "###,###,###,###.##";

   private final List<String> commands;
   private final MemoryAnalyzer analyzer;
   private final StringBuilder buffer;
   private final DecimalFormat format;
   private final Database database;
   private final Catalog catalog;
   private final SessionTime start;
   
   public SessionContext(SessionTime start, MemoryAnalyzer analyzer, Database database, Catalog catalog) {
      this.format = new DecimalFormat(DECIMAL_FORMAT);
      this.commands = new ArrayList<String>();      
      this.buffer = new StringBuilder();
      this.analyzer = analyzer;
      this.database = database;
      this.catalog = catalog;
      this.start = start;
   }
   
   public SessionTime getTime() {
      return start;
   } 
   
   public DecimalFormat getFormat() {
      return format;
   }   
   
   public MemoryAnalyzer getAnalyzer() {
      return analyzer;
   }
   
   public List<String> getHistory(){
      return commands;
   }  
   
   public StringBuilder getBuffer(){
      return buffer;
   }
   
   public Database getDatabase(){
      return database;
   }
   
   public Catalog getCatalog(){
      return catalog;
   }
}
