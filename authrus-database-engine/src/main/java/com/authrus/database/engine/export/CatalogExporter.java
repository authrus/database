package com.authrus.database.engine.export;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;

public class CatalogExporter {
   
   private final ExportFileBuilder builder;
   private final TableExporter exporter;   
   
   public CatalogExporter(ValueEscaper escaper) {
      this.exporter = new TableExporter(escaper);
      this.builder = new ExportFileBuilder();
   }
   
   public void export(Catalog catalog, File root) throws IOException {      
      Set<String> names = catalog.listTables();
           
      for(String name : names) {
         Table table = catalog.findTable(name);
         File file = builder.createFile(root, name);
         
         if(table != null) {
            exporter.export(table, file);
         }
      }
   }

}
