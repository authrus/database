package com.authrus.database.engine.export;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import com.authrus.database.engine.Table;

public class TableExporter {
   
   private final ValueEscaper escaper;
   
   public TableExporter(ValueEscaper escaper) { 
      this.escaper = escaper;
   }
   
   public void export(Table table, File file) throws IOException {      
      OutputStream result = new FileOutputStream(file);
      OutputStream compressor = new GZIPOutputStream(result);
      OutputStream buffer = new BufferedOutputStream(compressor, 8192);
      OutputStreamWriter writer = new OutputStreamWriter(buffer, "UTF-8");         
      TableAppender appender = new TableAppender(escaper, writer);
      
      try {
         appender.append(table);
      } finally {
         writer.close();
      }      
   }

}
